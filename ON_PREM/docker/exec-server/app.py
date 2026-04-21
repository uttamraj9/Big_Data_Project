"""
ITC Training Portal — Exec Server
  POST /run          stream Python file execution via SSH (NDJSON)
  POST /sql/run      execute SQL on PostgreSQL, return JSON rows
  GET  /sql/schema   return table/column schema for schema browser
  GET  /health       liveness probe
"""
import json
import os
import queue
import threading
import warnings
import uuid as _uuid

warnings.filterwarnings("ignore", category=DeprecationWarning)

import boto3
import botocore.exceptions
import paramiko
import psycopg2
import psycopg2.extras
from flask import Flask, request, Response, stream_with_context

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB

# ── SSH config (Python exec on Cloudera) ────────────────────────────
SSH_HOST   = os.getenv("SSH_HOST",   "13.41.167.97")
SSH_PORT   = int(os.getenv("SSH_PORT", "22"))
SSH_USER   = os.getenv("SSH_USER",   "consultant")
SSH_PASS   = os.getenv("SSH_PASS",   "")
PYTHON_BIN = os.getenv("PYTHON_BIN", "/home/consultant/pyenv/bin/python")
HOME_DIR   = os.getenv("HOME_DIR",   "/home/consultant")

# ── DB config (PostgreSQL on portal server) ──────────────────────────
DB_HOST = os.getenv("DB_HOST", "13.42.152.118")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER", "admin")
DB_PASS = os.getenv("DB_PASS", "admin123")
DB_NAME = os.getenv("DB_NAME", "testdb")

# ── S3 / Secrets Manager config ──────────────────────────────────────
# If AWS_SECRET_ARN is set the exec-server fetches credentials from
# Secrets Manager using the EC2 instance profile (no raw keys in K8s).
# Falls back to explicit AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars.
_AWS_SECRET_ARN = os.getenv("AWS_SECRET_ARN", "")
S3_REGION       = os.getenv("AWS_REGION", "eu-west-2")
S3_BUCKET       = os.getenv("AWS_S3_BUCKET", "")
_S3_ACCESS_KEY  = os.getenv("AWS_ACCESS_KEY_ID", "")
_S3_SECRET_KEY  = os.getenv("AWS_SECRET_ACCESS_KEY", "")


def _load_secrets_from_manager():
    """Fetch S3 credentials from Secrets Manager at startup (uses instance profile)."""
    global S3_BUCKET, S3_REGION, _S3_ACCESS_KEY, _S3_SECRET_KEY
    try:
        sm = boto3.client("secretsmanager", region_name=S3_REGION)
        raw = sm.get_secret_value(SecretId=_AWS_SECRET_ARN)["SecretString"]
        secret = json.loads(raw)
        _S3_ACCESS_KEY = secret.get("aws_access_key_id", _S3_ACCESS_KEY)
        _S3_SECRET_KEY = secret.get("aws_secret_access_key", _S3_SECRET_KEY)
        S3_BUCKET      = secret.get("s3_bucket", S3_BUCKET)
        S3_REGION      = secret.get("aws_region", S3_REGION)
    except Exception as e:
        print(f"[WARN] Could not load from Secrets Manager: {e} — falling back to env vars")


if _AWS_SECRET_ARN:
    _load_secrets_from_manager()


def _s3():
    if _S3_ACCESS_KEY and _S3_SECRET_KEY:
        return boto3.client(
            "s3", region_name=S3_REGION,
            aws_access_key_id=_S3_ACCESS_KEY,
            aws_secret_access_key=_S3_SECRET_KEY,
        )
    return boto3.client("s3", region_name=S3_REGION)


def _s3_key(program_id, module_id, topic_id, role, filename):
    """Path: {program}/{module}/{topic}/{admin|sme}/{filename}"""
    r = (role or "admin").lower()
    return f"{program_id}/{module_id}/{topic_id}/{r}/{filename}"


def _s3_enabled():
    return bool(S3_BUCKET)


def _db_conn(database=None):
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        database=database or DB_NAME,
        user=DB_USER, password=DB_PASS,
        connect_timeout=10,
    )


def _ensure_schema():
    """Add columns introduced after initial table creation."""
    try:
        conn = _db_conn()
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("""
            ALTER TABLE uploaded_slides
              ADD COLUMN IF NOT EXISTS uploaded_by_role VARCHAR(20) DEFAULT 'admin'
        """)
        cur.execute("""
            ALTER TABLE uploaded_slides
              ADD COLUMN IF NOT EXISTS s3_key VARCHAR(500)
        """)
        # Make content nullable now that S3 is primary storage
        cur.execute("""
            ALTER TABLE uploaded_slides
              ALTER COLUMN content DROP NOT NULL
        """)
        cur.close(); conn.close()
    except Exception:
        pass

_ensure_schema()


@app.after_request
def add_cors(resp: Response) -> Response:
    resp.headers["Access-Control-Allow-Origin"]  = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp


@app.route("/run", methods=["OPTIONS"])
@app.route("/stop", methods=["OPTIONS"])
@app.route("/sql/run", methods=["OPTIONS"])
@app.route("/sql/schema", methods=["OPTIONS"])
def options_handler():
    return Response("", status=204)


# ── OPTIONS for new routes ──────────────────────────────────────────
@app.route("/consultants", methods=["OPTIONS"])
@app.route("/consultants/register", methods=["OPTIONS"])
@app.route("/quiz/submit", methods=["OPTIONS"])
@app.route("/quiz/results", methods=["OPTIONS"])
def options_new():
    return Response("", status=204)


# ── Python exec via SSH ──────────────────────────────────────────────
@app.route("/run", methods=["POST"])
def run():
    data     = request.get_json(force=True, silent=True) or {}
    rel_path = data.get("path", "").strip().lstrip("/")
    if not rel_path:
        return Response(json.dumps({"error": "path required"}),
                        status=400, mimetype="application/json")
    abs_path = f"{HOME_DIR}/{rel_path}"

    def generate():
        q   = queue.Queue()
        EOF = object()

        def pump(recv_fn, msg_type):
            buf = ""
            try:
                while True:
                    chunk = recv_fn(4096)
                    if not chunk:
                        break
                    buf += chunk.decode("utf-8", errors="replace")
                    while "\n" in buf:
                        line, buf = buf.split("\n", 1)
                        q.put(json.dumps({"type": msg_type, "text": line + "\n"}))
                if buf:
                    q.put(json.dumps({"type": msg_type, "text": buf}))
            finally:
                q.put(EOF)

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(SSH_HOST, port=SSH_PORT,
                           username=SSH_USER, password=SSH_PASS,
                           timeout=10, banner_timeout=10)
            channel = client.get_transport().open_session()
            channel.exec_command(f"{PYTHON_BIN} -u {abs_path}")

            t_out = threading.Thread(target=pump, args=(channel.recv,        "stdout"), daemon=True)
            t_err = threading.Thread(target=pump, args=(channel.recv_stderr, "stderr"), daemon=True)
            t_out.start()
            t_err.start()

            done = 0
            while done < 2:
                item = q.get()
                if item is EOF:
                    done += 1
                else:
                    yield item + "\n"

            code = channel.recv_exit_status()
            yield json.dumps({"type": "done", "code": code}) + "\n"
        except Exception as exc:
            yield json.dumps({"type": "stderr", "text": f"Exec error: {exc}\n"}) + "\n"
            yield json.dumps({"type": "done",   "code": 1}) + "\n"
        finally:
            try:
                client.close()
            except Exception:
                pass

    return Response(
        stream_with_context(generate()),
        mimetype="text/plain",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/stop", methods=["POST"])
def stop():
    return Response(json.dumps({"ok": True}), mimetype="application/json")


# ── SQL execution ────────────────────────────────────────────────────
@app.route("/sql/run", methods=["POST"])
def sql_run():
    data     = request.get_json(force=True, silent=True) or {}
    query    = data.get("query", "").strip()
    database = data.get("database", DB_NAME)

    if not query:
        return Response(json.dumps({"error": "query required"}),
                        status=400, mimetype="application/json")

    try:
        conn = _db_conn(database)
        conn.autocommit = True
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(query)

        if cur.description:
            cols = [d.name for d in cur.description]
            rows = []
            for r in cur.fetchmany(2000):
                rows.append([str(v) if v is not None else None for v in r.values()])
            payload = {"columns": cols, "rows": rows, "rowcount": len(rows)}
        else:
            payload = {"columns": [], "rows": [], "rowcount": cur.rowcount}

        cur.close()
        conn.close()
        return Response(json.dumps(payload), mimetype="application/json")

    except Exception as exc:
        return Response(json.dumps({"error": str(exc)}),
                        status=400, mimetype="application/json")


# ── Schema browser ───────────────────────────────────────────────────
@app.route("/sql/schema", methods=["GET"])
def sql_schema():
    database = request.args.get("database", DB_NAME)
    try:
        conn = _db_conn(database)
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT table_name, column_name, data_type, ordinal_position
            FROM   information_schema.columns
            WHERE  table_schema = 'public'
            ORDER  BY table_name, ordinal_position
        """)
        columns = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        return Response(json.dumps({"database": database, "columns": columns}),
                        mimetype="application/json")
    except Exception as exc:
        return Response(json.dumps({"error": str(exc)}),
                        status=400, mimetype="application/json")


# ── Liveness probe ───────────────────────────────────────────────────
@app.route("/health")
def health():
    return {"status": "ok"}


# ── Consultant registration ──────────────────────────────────────────
@app.route("/consultants/register", methods=["POST"])
def register_consultant():
    data = request.get_json(force=True, silent=True) or {}
    name       = (data.get("name") or "").strip()
    email      = (data.get("email") or "").strip().lower()
    tech_stack = (data.get("tech_stack") or "").strip()
    cohort     = (data.get("cohort") or "").strip()
    if not name or not email or not tech_stack:
        return Response(json.dumps({"error": "name, email, tech_stack required"}),
                        status=400, mimetype="application/json")
    try:
        conn = _db_conn()
        conn.autocommit = True
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            INSERT INTO consultants (name, email, tech_stack, cohort)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (email) DO UPDATE SET name=EXCLUDED.name,
              tech_stack=EXCLUDED.tech_stack, cohort=EXCLUDED.cohort
            RETURNING id, name, email, tech_stack, cohort, created_at
        """, (name, email, tech_stack, cohort or None))
        row = dict(cur.fetchone())
        row["id"] = str(row["id"])
        row["created_at"] = str(row["created_at"])
        cur.close(); conn.close()
        return Response(json.dumps(row), mimetype="application/json")
    except Exception as exc:
        return Response(json.dumps({"error": str(exc)}), status=500, mimetype="application/json")


# ── List consultants ─────────────────────────────────────────────────
@app.route("/consultants", methods=["GET"])
def list_consultants():
    try:
        conn = _db_conn()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT id::text, name, email, tech_stack, cohort, created_at::text FROM consultants ORDER BY created_at DESC")
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return Response(json.dumps(rows), mimetype="application/json")
    except Exception as exc:
        return Response(json.dumps({"error": str(exc)}), status=500, mimetype="application/json")


# ── Submit quiz result ───────────────────────────────────────────────
@app.route("/quiz/submit", methods=["POST"])
def quiz_submit():
    data           = request.get_json(force=True, silent=True) or {}
    consultant_name  = (data.get("consultant_name") or "").strip()
    consultant_email = (data.get("consultant_email") or "").strip().lower()
    program_id     = (data.get("program_id") or "").strip()
    module_id      = (data.get("module_id") or "").strip()
    topic_id       = (data.get("topic_id") or "").strip()
    score          = int(data.get("score", 0))
    total          = int(data.get("total", 1))
    if not consultant_name or not program_id or not topic_id:
        return Response(json.dumps({"error": "consultant_name, program_id, topic_id required"}),
                        status=400, mimetype="application/json")
    pct = round(score / total * 100, 2) if total > 0 else 0
    try:
        conn = _db_conn()
        conn.autocommit = True
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            INSERT INTO quiz_results
              (consultant_name, consultant_email, program_id, module_id, topic_id, score, total, pct)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id::text, submitted_at::text
        """, (consultant_name, consultant_email or None, program_id, module_id, topic_id, score, total, pct))
        row = dict(cur.fetchone())
        cur.close(); conn.close()
        return Response(json.dumps({"ok": True, "id": row["id"], "pct": pct}), mimetype="application/json")
    except Exception as exc:
        return Response(json.dumps({"error": str(exc)}), status=500, mimetype="application/json")


# ── Get quiz results (admin) ─────────────────────────────────────────
@app.route("/quiz/results", methods=["GET"])
def quiz_results():
    topic_id = request.args.get("topic_id")
    try:
        conn = _db_conn()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if topic_id:
            cur.execute("""
                SELECT id::text, consultant_name, consultant_email, topic_id, score, total, pct, submitted_at::text
                FROM quiz_results WHERE topic_id=%s ORDER BY submitted_at DESC
            """, (topic_id,))
        else:
            cur.execute("""
                SELECT id::text, consultant_name, consultant_email, topic_id, score, total, pct, submitted_at::text
                FROM quiz_results ORDER BY submitted_at DESC LIMIT 200
            """)
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return Response(json.dumps(rows), mimetype="application/json")
    except Exception as exc:
        return Response(json.dumps({"error": str(exc)}), status=500, mimetype="application/json")


# ── OPTIONS for slide routes ─────────────────────────────────────────
@app.route("/slides/upload",          methods=["OPTIONS"])
@app.route("/slides/list",            methods=["OPTIONS"])
@app.route("/slides/file/<fid>",      methods=["OPTIONS", "DELETE"])
@app.route("/slides/migrate-to-s3",   methods=["OPTIONS"])
def slides_options(fid=None):
    return Response("", status=204)


# ── Upload slide/note file ───────────────────────────────────────────
@app.route("/slides/upload", methods=["POST"])
def slides_upload():
    program_id       = request.form.get("program_id",       "").strip()
    module_id        = request.form.get("module_id",        "").strip()
    topic_id         = request.form.get("topic_id",         "").strip()
    uploaded_by      = request.form.get("uploaded_by",      "SME").strip()
    uploaded_by_role = request.form.get("uploaded_by_role", "admin").strip().lower()

    if "file" not in request.files:
        return Response(json.dumps({"error": "file required"}), status=400, mimetype="application/json")
    if not program_id or not topic_id:
        return Response(json.dumps({"error": "program_id and topic_id required"}), status=400, mimetype="application/json")

    f         = request.files["file"]
    filename  = f.filename or "upload"
    ext       = filename.rsplit(".", 1)[-1].lower() if "." in filename else "bin"
    content   = f.read()
    file_size = len(content)
    mime      = MIME.get(ext, "application/octet-stream")

    # Upload to S3 when configured, else fall back to PostgreSQL BYTEA
    s3_key = None
    if _s3_enabled():
        s3_key = _s3_key(program_id, module_id or "", topic_id, uploaded_by_role, filename)
        try:
            _s3().put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=content,
                ContentType=mime,
                ContentDisposition=f'inline; filename="{filename}"',
            )
        except Exception as exc:
            return Response(json.dumps({"error": f"S3 upload failed: {exc}"}),
                            status=500, mimetype="application/json")

    try:
        conn = _db_conn()
        conn.autocommit = True
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            INSERT INTO uploaded_slides
              (program_id, module_id, topic_id, filename, file_type, file_size,
               content, uploaded_by, uploaded_by_role, s3_key)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id::text, filename, file_type, file_size, uploaded_by,
                      uploaded_by_role, s3_key, created_at::text
        """, (program_id, module_id or "", topic_id, filename, ext, file_size,
              None if _s3_enabled() else psycopg2.Binary(content),
              uploaded_by, uploaded_by_role, s3_key))
        row = dict(cur.fetchone())
        cur.close(); conn.close()
        return Response(json.dumps({"ok": True, **row}), mimetype="application/json")
    except Exception as exc:
        # Roll back S3 object if DB insert failed
        if s3_key:
            try: _s3().delete_object(Bucket=S3_BUCKET, Key=s3_key)
            except Exception: pass
        return Response(json.dumps({"error": str(exc)}), status=500, mimetype="application/json")


# ── List files for a topic ───────────────────────────────────────────
@app.route("/slides/list", methods=["GET"])
def slides_list():
    program_id = request.args.get("program_id", "")
    module_id  = request.args.get("module_id",  "")
    topic_id   = request.args.get("topic_id",   "")
    try:
        conn = _db_conn()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id::text, program_id, module_id, topic_id,
                   filename, file_type, file_size,
                   uploaded_by, COALESCE(uploaded_by_role,'admin') AS uploaded_by_role,
                   s3_key, created_at::text
            FROM   uploaded_slides
            WHERE  (%s = '' OR program_id = %s)
              AND  (%s = '' OR module_id  = %s)
              AND  (%s = '' OR topic_id   = %s)
            ORDER  BY uploaded_by_role, uploaded_by, created_at DESC
        """, (program_id, program_id, module_id, module_id, topic_id, topic_id))
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return Response(json.dumps(rows), mimetype="application/json")
    except Exception as exc:
        return Response(json.dumps({"error": str(exc)}), status=500, mimetype="application/json")


# ── Serve file content ───────────────────────────────────────────────
MIME = {
    "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "ppt":  "application/vnd.ms-powerpoint",
    "pdf":  "application/pdf",
    "txt":  "text/plain",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
}

@app.route("/slides/file/<fid>", methods=["GET"])
def slides_file(fid):
    try:
        conn = _db_conn()
        cur  = conn.cursor()
        cur.execute(
            "SELECT filename, file_type, s3_key, content FROM uploaded_slides WHERE id = %s::uuid",
            (fid,)
        )
        row = cur.fetchone()
        cur.close(); conn.close()
        if not row:
            return Response(json.dumps({"error": "not found"}), status=404, mimetype="application/json")

        filename, file_type, s3_key, db_content = row
        mime = MIME.get(file_type, "application/octet-stream")

        # S3 is primary; fall back to BYTEA for records uploaded before S3 migration
        if s3_key and _s3_enabled():
            obj     = _s3().get_object(Bucket=S3_BUCKET, Key=s3_key)
            content = obj["Body"].read()
        elif db_content:
            content = bytes(db_content)
        else:
            return Response(json.dumps({"error": "file content not found"}), status=404, mimetype="application/json")

        resp = Response(content, mimetype=mime)
        resp.headers["Content-Disposition"] = f'inline; filename="{filename}"'
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp
    except Exception as exc:
        return Response(json.dumps({"error": str(exc)}), status=500, mimetype="application/json")


# ── Delete file ──────────────────────────────────────────────────────
@app.route("/slides/file/<fid>", methods=["DELETE"])
def slides_delete(fid):
    try:
        conn = _db_conn()
        conn.autocommit = True
        cur  = conn.cursor()
        cur.execute(
            "DELETE FROM uploaded_slides WHERE id = %s::uuid RETURNING id, s3_key",
            (fid,)
        )
        deleted = cur.fetchone()
        cur.close(); conn.close()
        if not deleted:
            return Response(json.dumps({"error": "not found"}), status=404, mimetype="application/json")
        _, s3_key = deleted
        # Remove from S3 if stored there
        if s3_key and _s3_enabled():
            try: _s3().delete_object(Bucket=S3_BUCKET, Key=s3_key)
            except Exception: pass
        return Response(json.dumps({"ok": True}), mimetype="application/json")
    except Exception as exc:
        return Response(json.dumps({"error": str(exc)}), status=500, mimetype="application/json")


# ── Migrate existing BYTEA files → S3 (one-time, idempotent) ─────────
@app.route("/slides/migrate-to-s3", methods=["POST"])
def slides_migrate_to_s3():
    if not _s3_enabled():
        return Response(json.dumps({"error": "S3 not configured"}), status=400, mimetype="application/json")
    try:
        conn = _db_conn()
        conn.autocommit = True
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id::text, program_id, module_id, topic_id, filename,
                   file_type, COALESCE(uploaded_by_role,'admin') AS uploaded_by_role, content
            FROM   uploaded_slides
            WHERE  s3_key IS NULL AND content IS NOT NULL
        """)
        rows = cur.fetchall()
        migrated, failed = 0, []
        for r in rows:
            key = _s3_key(r["program_id"], r["module_id"], r["topic_id"],
                          r["uploaded_by_role"], r["filename"])
            try:
                _s3().put_object(
                    Bucket=S3_BUCKET, Key=key, Body=bytes(r["content"]),
                    ContentType=MIME.get(r["file_type"], "application/octet-stream"),
                    ContentDisposition=f'inline; filename="{r["filename"]}"',
                )
                cur.execute(
                    "UPDATE uploaded_slides SET s3_key=%s, content=NULL WHERE id=%s::uuid",
                    (key, r["id"])
                )
                migrated += 1
            except Exception as exc:
                failed.append({"id": r["id"], "file": r["filename"], "error": str(exc)})
        cur.close(); conn.close()
        return Response(json.dumps({"migrated": migrated, "failed": failed}), mimetype="application/json")
    except Exception as exc:
        return Response(json.dumps({"error": str(exc)}), status=500, mimetype="application/json")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8891, threaded=True)
