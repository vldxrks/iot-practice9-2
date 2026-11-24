FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY logic.py processor.py config.json ./
CMD ["python", "-u", "processor.py"]
