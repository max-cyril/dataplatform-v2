FROM python:3.9-slim-bullseye


LABEL version="1.0.0"
LABEL description="dbt image for dataplatform project"
LABEL build_date="2025-09"

RUN set -e && apt-get update && \
    apt-get install -y --no-install-recommends \
    bash \
    ca-certificates \
    git \
    jq \
    coreutils \ 
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*



COPY requirements.txt /tmp/requirements.txt

RUN python -m pip install --upgrade pip \
 && pip install -r /tmp/requirements.txt

ENV APP_NAME="dbt-dataplatform"
ENV APP_HOME="/usr/app/$APP_NAME"
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PATH=/root/.local/bin:$PATH \
    DBT_PROFILES_DIR=$APP_HOME/dbt\
    PYTHONIOENCODING=utf-8 \
    LANG=C.UTF-8

WORKDIR $APP_HOME/dbt

COPY dbt/ .

#COPY docker-entrypoint.sh /docker-entrypoint.sh
#RUN chmod +x /docker-entrypoint.sh

RUN useradd -m -d "$APP_HOME" dbt-dataplatform && \
    chown -R dbt-dataplatform "$APP_HOME"

USER dbt-dataplatform

CMD ["bash", "-c", "while true; do sleep 1000; done"]


#ENTRYPOINT [dbt build]
