import os
import asyncio

import httpx
from dotenv import load_dotenv

load_dotenv()


async def main() -> None:
    host = os.getenv("DATABRICKS_HOST", "")
    token = os.getenv("DATABRICKS_TOKEN", "")
    job_id = os.getenv("DATABRICKS_JID_BTC", "")
    print(f"host set: {bool(host)}, token set: {bool(token)}, job_id set: {bool(job_id)}")
    if not host or not token or not job_id:
        raise RuntimeError("missing DATABRICKS_HOST / DATABRICKS_TOKEN / DATABRICKS_JID_BTC")

    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(
            f"https://{host}/api/2.1/jobs/run-now",
            headers={"Authorization": f"Bearer {token}"},
            json={"job_id": int(job_id)},
        )
        print(r.status_code)
        print(r.text[:500])


if __name__ == "__main__":
    asyncio.run(main())
