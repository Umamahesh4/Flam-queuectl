# Create this file: queuectl_project/job_manager.py
import sqlite3
import json
import uuid
from datetime import datetime
from .database import get_db_conn  # <-- Relative
from .config import load_config  # <-- Relative

def enqueue_job(job_json_string: str) -> str:
    conn = get_db_conn()
    config = load_config()
    
    try:
        data = json.loads(job_json_string)
    except json.JSONDecodeError:
        return "Error: Invalid JSON format."

    if 'command' not in data:
        return "Error: 'command' field is required."

    now = datetime.utcnow().isoformat()
    job = {
        'id': data.get('id', str(uuid.uuid4())),
        'command': data['command'],
        'state': 'pending',
        'attempts': 0,
        'max_retries': data.get('max_retries', config.get('max_retries', 3)),
        'created_at': now,
        'updated_at': now,
        'run_at': data.get('run_at', now)
    }

    try:
        conn.execute(
            """
            INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at, run_at)
            VALUES (:id, :command, :state, :attempts, :max_retries, :created_at, :updated_at, :run_at)
            """,
            job
        )
        conn.commit()
        return f"Job enqueued with ID: {job['id']}"
    except sqlite3.IntegrityError:
        return f"Error: Job with ID {job['id']} already exists."
    finally:
        conn.close()

def get_status() -> dict:
    conn = get_db_conn()
    cursor = conn.cursor()
    
    cursor.execute("SELECT state, COUNT(*) FROM jobs GROUP BY state")
    job_states = {row['state']: row['COUNT(*)'] for row in cursor.fetchall()}
    
    cursor.execute("SELECT COUNT(*) FROM dlq")
    dlq_count = cursor.fetchone()[0]
    if dlq_count > 0:
        job_states['dead'] = dlq_count
        
    conn.close()
    return job_states

def list_jobs(state: str) -> list:
    conn = get_db_conn()
    if state == 'dead':
        cursor = conn.execute("SELECT * FROM dlq")
    else:
        cursor = conn.execute("SELECT * FROM jobs WHERE state = ?", (state,))
    
    jobs = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jobs

def retry_dlq_job(job_id: str) -> str:
    conn = get_db_conn()
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        
        with conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM dlq WHERE id = ?", (job_id,))
            job = cursor.fetchone()
            
            if not job:
                return f"Error: Job {job_id} not found in DLQ."

            now = datetime.utcnow().isoformat()
            
            conn.execute(
                """
                INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at, run_at)
                VALUES (?, ?, 'pending', 0, ?, ?, ?, ?)
                """,
                (job['id'], job['command'], job['max_retries'], job['created_at'], now, now)
            )
            
            conn.execute("DELETE FROM dlq WHERE id = ?", (job_id,))
            
        return f"Job {job_id} re-enqueued from DLQ."
    except sqlite3.IntegrityError:
        return f"Error: Job {job_id} might already exist in the main queue."
    except Exception as e:
        return f"An error occurred: {e}"
    finally:
        conn.close()

def get_job_logs(job_id: str) -> dict | None:
    conn = get_db_conn()
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, stdout, stderr, state, updated_at FROM jobs WHERE id = ?", (job_id,))
    job = cursor.fetchone()
    
    if job:
        conn.close()
        return dict(job)
        
    cursor.execute("SELECT id, stdout, stderr, state, failed_at as updated_at FROM dlq WHERE id = ?", (job_id,))
    job = cursor.fetchone()
    
    if job:
        conn.close()
        return dict(job)

    conn.close()
    return None