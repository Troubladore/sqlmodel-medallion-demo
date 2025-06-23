
FROM apache/airflow:2.9.1-python3.10

USER root

# Install Java, Spark, and PostgreSQL client
RUN apt-get update && \
    apt-get install -y default-jdk wget curl unzip postgresql-client && \
    wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.1-bin-hadoop3.tgz -C /opt/ && \
    ln -s /opt/spark-3.5.1-bin-hadoop3 /opt/spark && \
    rm spark-3.5.1-bin-hadoop3.tgz

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=/home/airflow/.local/bin:$PATH:$SPARK_HOME/bin

# Add PostgreSQL JDBC driver
RUN mkdir -p /opt/jdbc && \
    wget -O /opt/jdbc/postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

USER airflow

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
