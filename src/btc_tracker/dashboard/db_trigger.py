import os
import httpx
import logfire
from dotenv import load_dotenv


load_dotenv()

HOST = os.environ.get("DATABRICKS_HOST", "")
if HOST and "://" not in HOST:
    HOST = f"https://{HOST}"
TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
JOB_ID = os.environ.get("DATABRICKS_JID_BTC", "")

async def trigger_job(h):
    """Trigger the incremental job for tip h. Returns the run_id."""
    if not HOST or not TOKEN or not JOB_ID:
        raise RuntimeError("missing DATABRICKS_HOST / DATABRICKS_TOKEN / DATABRICKS_JID_BTC")
    with logfire.span("databricks.trigger", height=h, job_id=JOB_ID):
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(
                f"{HOST}/api/2.1/jobs/run-now",
                headers={"Authorization": f"Bearer {TOKEN}"},
                json={"job_id": int(JOB_ID)},
            )
            r.raise_for_status()
            run_id = r.json().get("run_id")
            logfire.info("databricks.triggered", height=h, run_id=run_id)
            return run_id




