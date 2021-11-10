FROM python:3.9

WORKDIR /datalakequeryingester

COPY . .

COPY ./log_config.yaml /opt/config/datalakequeryingester/log_config.yaml

RUN python3.9 -m pip install tox .

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 CMD [ "curl", "localhost:8080/healthy" ]

ENTRYPOINT [ "python3.9", "-m", "bloomberg.datalake.datalakequeryingester" ]
