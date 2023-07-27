FROM apache/airflow:2.5.1-python3.9 as builder

ARG TARGETARCH

USER root

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/common"

RUN  apt-get install -y apt-transport-https ca-certificates dirmngr \
  && apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 8919F6BD2B48D754 \
  && echo "deb https://packages.clickhouse.com/deb stable main" | sudo tee /etc/apt/sources.list.d/clickhouse.list \
  && echo "deb https://deb.debian.org/debian/ testing main contrib non-free" |  tee /etc/apt/sources.list.d/testing.list \
  && apt-get update \
  && apt-get install -y --no-install-recommends git clickhouse-client gcc g++ make librdkafka-dev \
  && apt-get purge -y --auto-remove \
  && rm -rf /var/lib/apt/lists/*

USER ${AIRFLOW_UID}

COPY requirements.txt .
RUN pip3 install -r requirements.txt

FROM builder

COPY ./dags  /opt/airflow/dags
COPY ./common  /opt/airflow/common
COPY --chmod=777 --chown=airflow:airflow ./dbt_clickhouse /opt/dbt_clickhouse

RUN dbt deps --project-dir /opt/dbt_clickhouse --profiles-dir /tmp
