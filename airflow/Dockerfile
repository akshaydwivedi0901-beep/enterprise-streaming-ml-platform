FROM apache/airflow:2.8.1

USER root

# Install Java + basic tools
RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    ca-certificates \
    && apt-get clean

# Set JAVA_HOME dynamically
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt