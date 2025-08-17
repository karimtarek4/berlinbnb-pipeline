FROM apache/airflow:2.8.1

# Install S3 provider (brings in boto3) and PostgreSQL client
RUN pip install \
    apache-airflow-providers-amazon \
    psycopg2-binary

# Install uv
RUN pip install uv

# Switch to root to install system dependencies
USER root
RUN apt-get update && apt-get install -y \
    git \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# Set workdir to airflow home
WORKDIR /opt/airflow

# Switch back to airflow user
USER airflow

# Copy the pyproject.toml file first
COPY pyproject.toml /opt/airflow/pyproject.toml

# Create venv and install only the dependencies (not the project itself)
RUN uv venv && uv pip install --requirement pyproject.toml

# Set environment variables so the venv is used by default
ENV VIRTUAL_ENV=/opt/airflow/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"