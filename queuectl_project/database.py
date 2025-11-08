# Create this file: queuectl_project/database.py
import sqlite3
import os
from .config import DB_PATH  # <-- Relative import works on Linux

def get_db_conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def init_db():
    conn = get_db_conn()
    cursor = conn.cursor()
    
    # Main jobs table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        command TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'pending',
        attempts INTEGER NOT NULL DEFAULT 0,
        max_retries INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        run_at TEXT NOT NULL DEFAULT (DATETIME('now')),
        stdout TEXT,
        stderr TEXT
    )
    ''')
    
    # Dead Letter Queue (DLQ) table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS dlq (
        id TEXT PRIMARY KEY,
        command TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'dead',
        attempts INTEGER NOT NULL,
        max_retries INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        failed_at TEXT NOT NULL,
        stdout TEXT,
        stderr TEXT
    )
    ''')
    
    cursor.execute('''
    CREATE INDEX IF NOT EXISTS idx_jobs_pending_run_at 
    ON jobs (state, run_at) 
    WHERE state = 'pending';
    ''')
    
    conn.commit()
    conn.close()