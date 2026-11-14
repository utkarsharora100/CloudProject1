FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app
COPY common/ ./common/
COPY customer_service/*.py ./customer_service/
COPY order_service/*.py ./order_service/
COPY task_scheduler/*.py ./task_scheduler/
COPY payment_service/*.py ./payment_service/
COPY kitchen_workflow/*.py ./kitchen_workflow/
COPY restaurant_service/*.py ./restaurant_service/
COPY delivery_assignment_service/*.py ./delivery_assignment_service/
COPY local/environment.py ./local/environment.py
EXPOSE 8082
CMD ["python", "-m", "delivery_assignment_service.das"]
