from fastapi import FastAPI, HTTPException
from celery_task import mul
from datetime import datetime

app = FastAPI()

@app.post("/multiply/")
async def schedule_task(a: int, b: int):
    task = mul.delay(a, b)  # This will work correctly
    return {"message": f"The multiplication is in process", "task_id": task.id}
