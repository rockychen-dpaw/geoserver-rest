# Prepare the base environment.
FROM python:3.12.3-slim-bookworm AS builder_base_geoserver_healthcheck
LABEL org.opencontainers.image.authors=asi@dbca.wa.gov.au
LABEL org.opencontainers.image.source=https://github.com/dbca-wa/geoserver
RUN apt-get update -y \
  && apt-get upgrade -y \
  && apt-get install -y wget libmagic-dev gcc binutils python3-dev libpq-dev \
  && rm -rf /var/lib/apt/lists/* \
  && pip install --upgrade pip

#install and config poetry
WORKDIR /app
ENV POETRY_VERSION=1.5.1
RUN pip install "poetry==$POETRY_VERSION"

#update add user
RUN groupadd -r geoserver -g 9999
RUN useradd -l -m -d /home/geoserver -u 9999 --gid 9999 -s /bin/bash -G geoserver geoserver

# Install Python libs from pyproject.toml.
WORKDIR /app
# Install the project.
COPY poetry.lock pyproject.toml ./

RUN poetry config virtualenvs.create false \
  && poetry install --only main --no-interaction --no-ansi

COPY reports.html notify_email.html ./
COPY geoserver_rest ./geoserver_rest

RUN echo "#!/bin/bash \n\
if [[ \"\${GEOSERVER_URLS}\" == \"\" ]]; then \n\
    cd /app && python -m geoserver_rest.geoserverhealthcheck \n\
else \n\
    cd /app && python -m geoserver_rest.geoservershealthcheck \n\
fi \n\
" > run_healthcheck

RUN chmod 555 run_healthcheck

RUN chown -R geoserver:geoserver /app

# Run the application as the geoserver user.
USER geoserver
CMD ["./run_healthcheck"]
