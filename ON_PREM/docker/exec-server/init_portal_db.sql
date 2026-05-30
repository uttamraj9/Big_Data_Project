-- ============================================================
-- ITC Training Portal — itcportal database bootstrap
-- Run once after creating the Docker container:
--   docker exec -i itc-portal-db psql -U portal_admin -d itcportal < init_portal_db.sql
-- ============================================================

-- ── Extensions ───────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ── Users ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS portal_users (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name          TEXT NOT NULL,
    email         TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role          TEXT NOT NULL DEFAULT 'consultant'
                  CHECK (role IN ('admin','sme','consultant')),
    tech_stack    TEXT,
    cohort        TEXT,
    programs      JSONB NOT NULL DEFAULT '["big-data"]',
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login    TIMESTAMPTZ
);

-- ── Batches / Cohorts ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS batches (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name          TEXT NOT NULL,
    program_id    TEXT NOT NULL,
    start_date    DATE NOT NULL,
    end_date      DATE,
    created_by    UUID REFERENCES portal_users(id),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS batch_members (
    batch_id      UUID REFERENCES batches(id) ON DELETE CASCADE,
    user_id       UUID REFERENCES portal_users(id) ON DELETE CASCADE,
    joined_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (batch_id, user_id)
);

CREATE TABLE IF NOT EXISTS batch_smes (
    batch_id      UUID REFERENCES batches(id) ON DELETE CASCADE,
    sme_id        UUID REFERENCES portal_users(id) ON DELETE CASCADE,
    PRIMARY KEY (batch_id, sme_id)
);

-- ── Topic Progress ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS topic_progress (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id                 UUID NOT NULL REFERENCES portal_users(id) ON DELETE CASCADE,
    program_id              TEXT NOT NULL,
    module_id               TEXT NOT NULL,
    topic_id                TEXT NOT NULL,
    slides_viewed           INT NOT NULL DEFAULT 0,
    total_slides            INT NOT NULL DEFAULT 0,
    slides_pct              NUMERIC(5,2) NOT NULL DEFAULT 0,
    quiz_score              INT,
    quiz_total              INT,
    quiz_pct                NUMERIC(5,2),
    quiz_completed_at       TIMESTAMPTZ,
    assignment_submitted    BOOLEAN NOT NULL DEFAULT FALSE,
    assignment_submitted_at TIMESTAMPTZ,
    topic_completed         BOOLEAN NOT NULL DEFAULT FALSE,
    completed_at            TIMESTAMPTZ,
    last_accessed           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, program_id, module_id, topic_id)
);

-- ── Assignment Submissions ────────────────────────────────────
CREATE TABLE IF NOT EXISTS assignment_submissions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID NOT NULL REFERENCES portal_users(id) ON DELETE CASCADE,
    program_id      TEXT NOT NULL,
    module_id       TEXT NOT NULL,
    topic_id        TEXT NOT NULL,
    submission_text TEXT,
    file_url        TEXT,
    status          TEXT NOT NULL DEFAULT 'submitted'
                    CHECK (status IN ('submitted','reviewed','returned','graded')),
    grade           TEXT,
    graded_by       UUID REFERENCES portal_users(id),
    graded_at       TIMESTAMPTZ,
    submitted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── SME Remarks ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sme_remarks (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    consultant_id   UUID NOT NULL REFERENCES portal_users(id) ON DELETE CASCADE,
    sme_id          UUID NOT NULL REFERENCES portal_users(id),
    program_id      TEXT NOT NULL,
    module_id       TEXT NOT NULL,
    topic_id        TEXT NOT NULL,
    section         TEXT NOT NULL CHECK (section IN ('slides','quiz','assignment','general')),
    remark          TEXT NOT NULL,
    rating          SMALLINT CHECK (rating BETWEEN 1 AND 5),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Login Activity ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS login_activity (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID REFERENCES portal_users(id) ON DELETE SET NULL,
    email       TEXT,
    ip_address  TEXT,
    logged_in_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Legacy quiz_results (kept for compatibility) ──────────────
CREATE TABLE IF NOT EXISTS quiz_results (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    consultant_name  TEXT NOT NULL,
    consultant_email TEXT,
    program_id       TEXT,
    module_id        TEXT,
    topic_id         TEXT NOT NULL,
    score            INT NOT NULL,
    total            INT NOT NULL,
    pct              NUMERIC(5,2),
    submitted_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Legacy consultants table (compatibility) ──────────────────
CREATE TABLE IF NOT EXISTS consultants (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name       TEXT NOT NULL,
    email      TEXT UNIQUE NOT NULL,
    tech_stack TEXT,
    cohort     TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Legacy uploaded_slides (compatibility) ────────────────────
CREATE TABLE IF NOT EXISTS uploaded_slides (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    program_id       TEXT,
    module_id        TEXT,
    topic_id         TEXT,
    filename         TEXT NOT NULL,
    file_type        TEXT NOT NULL,
    content          BYTEA,
    s3_key           TEXT,
    uploaded_by      TEXT,
    uploaded_by_role TEXT DEFAULT 'admin',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- SAMPLE DATA
-- ============================================================

-- NOTE: Admin and SME accounts are NOT stored here.
-- They authenticate via Microsoft login (Azure AD @informationtechconsultants.com).
-- Only consultant accounts live in this database.

-- Test Consultant: John Doe  (password: Consultant@ITC2026)
INSERT INTO portal_users (name, email, password_hash, role, programs) VALUES
  ('John Doe',
   'john.doe@informationtechconsultants.co.uk',
   '$2b$12$kPqUre4m.2qs38L4B3Rzu.zIKkOL4NWjZXXCpj7H29aILOfkhiWDi',
   'consultant',
   '["big-data"]')
ON CONFLICT (email) DO NOTHING;

-- Sample batch (created_by left NULL since admin is not in portal_users)
INSERT INTO batches (name, program_id, start_date, end_date)
VALUES ('April 2026 — Big Data', 'big-data', '2026-04-14', '2026-06-27')
ON CONFLICT DO NOTHING;

-- Add John Doe to the batch
INSERT INTO batch_members (batch_id, user_id)
SELECT b.id, u.id
FROM   batches b, portal_users u
WHERE  b.name = 'April 2026 — Big Data'
AND    u.email = 'john.doe@informationtechconsultants.co.uk'
ON CONFLICT DO NOTHING;

-- Sample topic progress for John
INSERT INTO topic_progress
  (user_id, program_id, module_id, topic_id,
   slides_viewed, total_slides, slides_pct,
   quiz_score, quiz_total, quiz_pct, quiz_completed_at,
   last_accessed)
SELECT id, 'big-data', 'linux', 'linux-basics',
       8, 10, 80.00,
       7, 10, 70.00, NOW() - INTERVAL '1 day',
       NOW() - INTERVAL '1 day'
FROM   portal_users WHERE email = 'john.doe@informationtechconsultants.co.uk'
ON CONFLICT DO NOTHING;

-- SME remarks are inserted by SMEs after they log in via Microsoft SSO.
-- No sample remark inserted here since SME IDs come from Azure AD at runtime.

COMMIT;
