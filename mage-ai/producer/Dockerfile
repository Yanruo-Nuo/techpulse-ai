FROM python:3.9-slim

WORKDIR /app

# 在香港地域，安装这些编译依赖会非常快
RUN apt-get update && apt-get install -y \
    build-essential \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY metrics.py .
COPY scrapers/ scrapers/

EXPOSE 8001

CMD ["python", "main.py"]
