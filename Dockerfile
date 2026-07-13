#be  Prepare the base environment.
FROM dhi.io/python:3.12-debian13-dev AS build-stage
LABEL org.opencontainers.image.authors=asi@dbca.wa.gov.au

RUN apt-get update -y \
  && apt-get upgrade -y \
  && apt-get install -y passwd \
  && rm -rf /var/lib/apt/lists/* \
  && pip install --upgrade pip

#update add user
RUN groupadd -r geoserver -g 9999
RUN useradd -l -m -d /home/geoserver -u 9999 --gid 9999 -s /bin/bash -G geoserver geoserver

# uv install
# grab /uvx too if you need it
COPY --from=ghcr.io/astral-sh/uv:0.11.1 /uv /bin/

#install and co
WORKDIR /app
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
COPY uv.lock pyproject.toml ./
RUN uv sync --no-group dev --link-mode=copy --compile-bytecode --no-python-downloads --frozen --no-install-project
RUN rm -f uv.lock
RUN rm -rf pyproject.toml

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

# Install Python libs from pyproject.toml.
# Install the project.

COPY reports.html notify_email.html gwclayers.html ./
COPY geoserver_rest ./geoserver_rest
COPY bin ./bin

RUN chown -R geoserver:geoserver /app

##################################################################################
FROM dhi.io/python:3.12-debian13-dev AS runtime-stage
#FROM python:3.12.10-slim-bookworm AS runtime-stage
LABEL org.opencontainers.image.authors=asi@dbca.wa.gov.au

# Copy the user & usergroup
COPY --from=build-stage /etc/group /etc/
COPY --from=build-stage /etc/passwd /etc/

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

# Copy over the built project and virtualenv
COPY --from=build-stage --chown=app:app /app /app

WORKDIR /app
# Run the application as the non-root user.
USER geoserver
CMD ["./bin/healthcheck.sh"]
