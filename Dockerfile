#be  Prepare the base environment.
FROM dhi.io/python:3.12-debian13-dev AS build-stage
LABEL org.opencontainers.image.authors=asi@dbca.wa.gov.au

RUN apt-get update -y \
  && apt-get upgrade -y \
  && apt-get install -y passwd curl\
  && rm -rf /var/lib/apt/lists/* \
  && pip install --upgrade pip

# Docker automatically populates these arguments during multi-platform builds
ARG TARGETARCH
ARG SUPERCRONIC_VERSION=v0.2.49
ARG SUPERCRONIC_SHA1SUM
ARG SUPERCRONIC

# Map Docker TARGETARCH to Supercronic binary suffix
RUN set -x \
    && if [ "$TARGETARCH" = "amd64" ]; then \
        SUPERCRONIC=supercronic-linux-amd64; \
        SUPERCRONIC_SHA1SUM=a53ae236602c7338aba3fbaff40bda6300eae3b9fedb8261eb06cfe3724430c1; \
       elif [ "$TARGETARCH" = "arm64" ]; then \
        SUPERCRONIC=supercronic-linux-arm64; \
        SUPERCRONIC_SHA1SUM=02aa0cb229ba09050cba6638059dadb9eedc2276632ea43d6a57a2f8c1629dd5; \
       elif [ "$TARGETARCH" = "arm" ]; then \
        SUPERCRONIC=supercronic-linux-arm; \
        SUPERCRONIC_SHA1SUM=d961592036e9f87a75e18cf4a7fcb78d1caa5732f57ada3a952906d2d3a4076c; \
       elif [ "$TARGETARCH" = "386" ]; then \
        SUPERCRONIC=supercronic-linux-386; \
        SUPERCRONIC_SHA1SUM=db302b8ce2be799937780f1baa41ebdf2bd67aa394ee1b2ae0e1e19c3117f6b8; \
       else \
        echo "Unsupported architecture: $TARGETARCH" && exit 1; \
       fi \
    && curl -fsSLO "https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/$SUPERCRONIC" \
    && echo "${SUPERCRONIC_SHA1SUM}  ${SUPERCRONIC}" | sha256sum -c  \
    && mv "./${SUPERCRONIC}" "/usr/bin/supercronic" \
    && chmod +x /usr/bin/supercronic 

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

# Copy supercronic
COPY --from=build-stage /usr/bin/supercronic /usr/bin


ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

# Copy over the built project and virtualenv
COPY --from=build-stage --chown=app:app /app /app

WORKDIR /app
# Run the application as the non-root user.
USER geoserver
CMD ["./bin/healthcheck.sh"]
