# Create this file: queuectl_project/worker.py
import time
import subprocess
import os
import signal
import sys
import multiprocessing
import sqlite3
from datetime import datetime, timedelta
from .database import get_db_conn  # <-- Relative
from .config import load_config, PID_FILE  # <-- Relative

shutdown_flag = multiprocessing.Event()

def signal_handler(sig, frame):
    print(f"Worker {os.getpid()}: Received shutdown signal. Finishing current job...")
    shutdown_flag.set()

def start_workers(count: int):
    pids = []
    print(f"Starting {count} worker(s)...")
    for _ in range(count):
        process = multiprocessing.Process(target=run_worker_loop, daemon=False)
        process.start()
        pids.append(process.pid)
        print(f"Started worker with PID: {process.pid}")

    with open(PID_FILE, 'w') as f:
        for pid in pids:
            f.write(f"{pid}\n")

def stop_workers():
    if not os.path.exists(PID_FILE):
        print("No workers seem to be running (PID file not found).")
        return

    print("Sending graceful shutdown (SIGTERM) to all workers...")
    with open(PID_FILE, 'r') as f:
        pids = [int(pid.strip()) for pid in f.readlines()]

    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
            print(f"Sent SIGTERM to PID {pid}")
        except ProcessLookupError:
            print(f"Worker PID {pid} not found. Already stopped?")
        except Exception as e:
            print(f"Could not stop PID {pid}: {e}")
    
    if os.path.exists(PID_FILE):
        os.remove(PID_FILE)
    print("All workers stopped.")

def get_active_workers() -> int:
    if not os.path.exists(PID_FILE):
        return 0
    
    count = 0
    valid_pids = []
    with open(PID_FILE, 'r') as f:
        pids = [int(pid.strip()) for pid in f.readlines()]

    for pid in pids:
        try:
            os.kill(pid, 0)
            count += 1
            valid_pids.append(pid)
        except OSError:
            pass 

    with open(PID_FILE, 'w') as f:
        for pid in valid_pids:
            f.write(f"{pid}\n")
            
    return count

def run_worker_loop():
    signal.signal(signal.SIGTERM, signal_handler)
    pid = os.getpid()
    
    while not shutdown_flag.is_set():
        job = claim_job()
        if job:
            print(f"[Worker {pid}] Processing job: {job['id']} (Attempt {job['attempts'] + 1})")
            execute_job(job)
        else:
            time.sleep(1) 
    
    print(f"[Worker {pid}] Shutting down.")
    sys.exit(0)

def claim_job() -> dict | None:
    conn = get_db_conn()
    try:
        with conn:
            conn.execute("BEGIN IMMEDIATE;")
            cursor = conn.cursor()
            
            cursor.execute(
                """
                SELECT * FROM jobs 
                WHERE state = 'pending' AND run_at <= DATETIME('now')
                ORDER BY created_at ASC 
                LIMIT 1
                """
            )
            job = cursor.fetchone()
            
            if job:
                now = datetime.utcnow().isoformat()
                conn.execute(
                    """
                    UPDATE jobs 
                    SET state = 'processing', updated_at = ? 
                    WHERE id = ?
                    """,
                    (now, job['id'])
                )
                return dict(job)
        
        return None
        
    except sqlite3.OperationalError as e:
        if "database is locked" in str(e):
            return None
        print(f"Database error in worker {os.getpid()}: {e}")
        return None
    except Exception as e:
        print(f"Error claiming job in worker {os.getpid()}: {e}")
        return None
    finally:
        conn.close()

def execute_job(job: dict):
    try:
        result = subprocess.run(
            job['command'], 
            shell=True, 
            check=True, 
            capture_output=True, 
            text=True,
            timeout=300 
        )
        print(f"Job {job['id']} completed successfully.")
        update_job_state(job['id'], 'completed', stdout=result.stdout, stderr=result.stderr)

    except subprocess.CalledProcessError as e:
        print(f"Job {job['id']} failed with exit code {e.returncode}. Stderr: {e.stderr}")
        handle_job_failure(job, stdout=e.stdout, stderr=e.stderr)
    except subprocess.TimeoutExpired as e:
        print(f"Job {job['id']} timed out.")
        handle_job_failure(job, stdout=e.stdout, stderr=e.stderr)
    except Exception as e:
        print(f"Job {job['id']} failed with error: {e}")
        handle_job_failure(job, stderr=str(e))

def handle_job_failure(job: dict, stdout: str = None, stderr: str = None):
    conn = get_db_conn()
    config = load_config()
    
    current_attempts = job['attempts'] + 1
    max_retries = job['max_retries']
    
    if current_attempts > max_retries:
        print(f"Job {job['id']} exceeded max retries. Moving to DLQ.")
        try:
            with conn:
                conn.execute(
                    """
                    INSERT INTO dlq (id, command, state, attempts, max_retries, created_at, failed_at, stdout, stderr)
                    VALUES (?, ?, 'dead', ?, ?, ?, ?, ?, ?)
                    """,
                    (job['id'], job['command'], current_attempts, max_retries, 
                     job['created_at'], datetime.utcnow().isoformat(), stdout, stderr)
                )
                conn.execute("DELETE FROM jobs WHERE id = ?", (job['id'],))
        except Exception as e:
            print(f"Failed to move job {job['id']} to DLQ: {e}")
            
    else:
        base_seconds = config.get('backoff_base', 2)
        delay_seconds = base_seconds ** (current_attempts - 1)
        run_at = datetime.utcnow() + timedelta(seconds=delay_seconds)
        
        print(f"Job {job['id']} scheduling retry {current_attempts}/{max_retries} in {delay_seconds}s.")
        
        try:
            conn.execute(
                """
                UPDATE jobs
                SET state = 'pending', attempts = ?, run_at = ?, updated_at = ?, stdout = ?, stderr = ?
                WHERE id = ?
                """,
                (current_attempts, run_at.isoformat(), datetime.utcnow().isoformat(), stdout, stderr, job['id'])
            )
            conn.commit()
        except Exception as e:
            print(f"Failed to schedule retry for job {job['id']}: {e}")
            
    conn.close()

def update_job_state(job_id: str, new_state: str, stdout: str = None, stderr: str = None):
    conn = get_db_conn()
    try:
        conn.execute(
            """
            UPDATE jobs 
            SET state = ?, updated_at = ?, stdout = ?, stderr = ? 
            WHERE id = ?
            """,
            (new_state, datetime.utcnow().isoformat(), stdout, stderr, job_id)
        )
        conn.commit()
    except Exception as e:
        print(f"Failed to update job {job_id} state: {e}")
    finally:
        conn.close()