# Create this file: queuectl_project/cli.py
import click
import json
from . import database, job_manager, worker as worker_logic, config as config_logic

@click.group()
def main_cli():
    """
    QueueCTL: A CLI-based background job queue system.
    """
    database.init_db()

# --- Enqueue Commands ---

@main_cli.command()
@click.argument('job_json_string')
@click.option('--run-at', help='ISO 8601 time to run the job (e.g., 2025-11-05T17:00:00Z)')
def enqueue(job_json_string, run_at):
    """
    Add a new job to the queue.
    
    Example:
    queuectl enqueue '{"id":"job1", "command":"echo hello"}'
    queuectl enqueue '{"command":"sleep 10"}' --run-at '2025-12-01T10:00:00Z'
    """
    try:
        data = json.loads(job_json_string)
    except json.JSONDecodeError:
        click.echo("Error: Invalid JSON format.")
        return

    if run_at:
        data['run_at'] = run_at
    
    message = job_manager.enqueue_job(json.dumps(data))
    click.echo(message)

# --- Worker Commands ---

@main_cli.group()
def worker():
    """Manage worker processes."""
    pass

@worker.command()
@click.option('--count', default=1, help='Number of worker processes to start.')
def start(count):
    """Start one or more worker processes in the background."""
    worker_logic.start_workers(count)  # <-- No 'worker_logic'

@worker.command()
def stop():
    """Stop all running worker processes gracefully."""
    worker_logic.stop_workers()  # <-- No 'worker_logic'

# --- Status & List Commands ---

@main_cli.command()
def status():
    """Show summary of all job states & active workers."""
    job_states = job_manager.get_status()
    active_workers = worker_logic.get_active_workers()  # <-- No 'worker_logic'
    
    click.echo("--- Worker Status ---")
    click.echo(f"Active Workers: {active_workers}\n")
    
    click.echo("--- Job Status ---")
    if not job_states:
        click.echo("No jobs in the system.")
        return
        
    for state, count in job_states.items():
        click.echo(f"{state.title()}: {count}")

@main_cli.command()
@click.option('--state', default='pending', 
              type=click.Choice(['pending', 'processing', 'completed', 'failed', 'dead']), 
              help='Filter jobs by state.')
def list(state):
    """List jobs by their state."""
    jobs = job_manager.list_jobs(state)
    if not jobs:
        click.echo(f"No jobs found with state: {state}")
        return
    click.echo(json.dumps(jobs, indent=2))

# --- DLQ Commands ---

@main_cli.group()
def dlq():
    """Manage the Dead Letter Queue (DLQ)."""
    pass

@dlq.command(name='list')
def dlq_list():
    """View all jobs in the DLQ."""
    jobs = job_manager.list_jobs('dead')
    if not jobs:
        click.echo("DLQ is empty.")
        return
    click.echo(json.dumps(jobs, indent=2))

@dlq.command()
@click.argument('job_id')
def retry(job_id):
    """Re-enqueue a specific job from the DLQ."""
    message = job_manager.retry_dlq_job(job_id)
    click.echo(message)

# --- Config Commands ---

@main_cli.group()
def config():
    """Manage configuration."""
    pass

@config.command()
@click.argument('key', type=click.Choice(['max_retries', 'backoff_base']))
@click.argument('value', type=int)
def set(key, value):
    """
    Set a configuration value.
    
    KEY: max_retries | backoff_base
    VALUE: integer
    """
    conf = config_logic.load_config()  # <-- No 'config_logic'
    conf[key] = value
    config_logic.save_config(conf)  # <-- No 'config_logic'
    click.echo(f"Set {key} = {value}")

@config.command()
def show():
    """Show the current configuration."""
    conf = config_logic.load_config()  # <-- No 'config_logic'
    click.echo(json.dumps(conf, indent=2))

# --- Bonus Log Command ---

@main_cli.command()
@click.argument('job_id')
def logs(job_id):
    """(Bonus Feature) View the stdout/stderr for a job."""
    job = job_manager.get_job_logs(job_id)
    if not job:
        click.echo(f"Error: Job {job_id} not found.")
        return

    click.echo(f"--- Logs for Job: {job['id']} ---")
    click.echo(f"State: {job['state']}")
    click.echo(f"Last Update: {job['updated_at']}")
    click.echo("\n--- STDOUT ---")
    click.echo(job['stdout'] if job['stdout'] else "[No stdout]")
    click.echo("\n--- STDERR ---")
    click.echo(job['stderr'] if job['stderr'] else "[No stderr]")

# NO 'if __name__ == "__main__"' block needed