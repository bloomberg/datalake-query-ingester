FROM python:3.9

# Necessary to install BBSM packages on the VPN
ENV PIP_INDEX_URL="https://artprod.dev.bloomberg.com/artifactory/api/pypi/bloomberg-pypi/simple"

WORKDIR /datalakequeryingester

COPY . .

COPY ./log_config.yaml /opt/config/datalakequeryingester/log_config.yaml

RUN python3.9 -m pip install tox . --trusted-host artprod.dev.bloomberg.com --index-url=$PIP_INDEX_URL

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 CMD [ "curl", "localhost:8080/healthy" ]

ENTRYPOINT [ "python3.9", "-m", "bloomberg.datalake.datalakequeryingester" ]
