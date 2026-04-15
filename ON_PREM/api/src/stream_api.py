import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI
from dotenv import load_dotenv


app = FastAPI()

# Load environment variables from .env file
load_dotenv()

# Define your PostgreSQL connection parameters
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
TABLE_NAME = os.getenv("TABLE_NAME")


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        cursor_factory=RealDictCursor
    )

@app.get("/data")
def get_data(offset: int = 0, limit: int = 100):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {TABLE_NAME} LIMIT %s OFFSET %s", (limit, offset))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return json.loads(json.dumps(rows, default=str))

if __name__ == '__main__':
    import uvicorn
    uvicorn.run("stream_api:app", host='0.0.0.0', port=5310, reload=True)