FROM python:3.12-slim
WORKDIR /function
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
CMD ["functions-framework","--target=deliver_notification","--signature-type=cloudevent","--port=8080"]
