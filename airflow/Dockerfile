FROM apache/airflow:2.8.0
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential  \
    python3-dev \
    libpq-dev \
    libopenmpi-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements.txt

USER airflow

RUN pip install --user -r requirements.txt

EXPOSE 8080
EXPOSE 8793
EXPOSE 5555
EXPOSE 8974
EXPOSE 6379