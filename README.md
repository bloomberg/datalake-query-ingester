# Datalake Query Ingester

Python microservice that receives datalake metadata through POSTs to /add_query_ingest and sends them to a Kafka topic.

## Menu

- [Rationale](#rationale)
- [Quick start](#quick-start)
- [Building](#building)
- [Installation](#installation)
- [Contributions](#contributions)
- [License](#license)
- [Code of Conduct](#code-of-conduct)
- [Security Vulnerability Reporting](#security-vulnerability-reporting)

## Rationale

The purpose of this service is to provide a way of producing Kafka messages from datalake query events, that will be consumed by [datalake-query-pg-consumer](link_to_repo).

This is part of a datalake query metadata ingestion and analysis pipeline. You can find more about that [here](to_be_added).

## Quick Start

To run the service locally, along with supporting services for testing, just run `docker-compose up datalakequeryingester`.
Similarlly, for tests run `docker-compose run tests`.

## Building
To build this project run `docker build -f Dockerfile -t datalakequeryingester:<your tag>`

OR

Run `docker-compose build`, which will create the image `datalakequeryingester:build`. Use `docker image tag datalakequeryingester:build datalakequeryingester:<your tag>` to change its tag.

## Installation

This is meant to be used with [Trino](https://github.com/trinodb/trino) and models data based on Trino's query metrics. This has been tested with [Trino 363](https://github.com/trinodb/trino/releases/tag/363), backwards or forwards compatibility is not guaranteed.

The service is meant to be deployed with k8s. Configuration is passed with environment variables:
- KAFKA_BROKERS
- DATALAKEQUERYINGESTER_KAFKA_TOPIC
- DATALAKEQUERYINGESTER_KAFKA_GROUP_ID

An example config can be found in `docker-compose.yaml > datalakequeryingester`.

Allows authentication through an API Key passed through the `X-API-Key` header. By default this does nothing. Implement a custom authentication/authorization mechanism by adding it to `_apik.py`.

## Contributions

We :heart: contributions.

Have you had a good experience with this project? Why not share some love and contribute code, or just let us know about any issues you had with it?

We welcome issue reports [here](../../issues); be sure to choose the proper issue template for your issue, so that we can be sure you're providing the necessary information.

Before sending a [Pull Request](../../pulls), please make sure you read our
[Contribution Guidelines](https://github.com/bloomberg/.github/blob/master/CONTRIBUTING.md).

## License

Please read the [LICENSE](LICENSE) file.

## Code of Conduct

This project has adopted a [Code of Conduct](https://github.com/bloomberg/.github/blob/master/CODE_OF_CONDUCT.md).
If you have any concerns about the Code, or behavior which you have experienced in the project, please
contact us at opensource@bloomberg.net.

## Security Vulnerability Reporting

If you believe you have identified a security vulnerability in this project, please send email to the project
team at opensource@bloomberg.net, detailing the suspected issue and any methods you've found to reproduce it.

Please do NOT open an issue in the GitHub repository, as we'd prefer to keep vulnerability reports private until
we've had an opportunity to review and address them.
