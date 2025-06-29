# Use the official Airflow image as base
FROM apache/airflow:2.7.0-python3.10

# Switch to root user to install system dependencies
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    wget \
    curl \
    unzip \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Switch back to airflow user
USER airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Create necessary directories
RUN mkdir -p /opt/airflow/dags \
    && mkdir -p /opt/airflow/logs \
    && mkdir -p /opt/airflow/plugins \
    && mkdir -p /opt/airflow/config

# Copy custom configurations
COPY airflow.cfg /opt/airflow/airflow.cfg

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh

# Switch to root to make script executable
USER root
RUN chmod +x /entrypoint.sh

# Switch back to airflow user
USER airflow

# Set working directory
WORKDIR /opt/airflow

# Set entrypoint
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"] 