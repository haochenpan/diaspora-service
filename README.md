# Diaspora Event Fabric Service

[![Docs Status](https://github.com/haochenpan/diaspora-service/actions/workflows/docs.yml/badge.svg)](https://github.com/haochenpan/diaspora-service/actions/workflows/docs.yml)
[![Tests Status](https://github.com/haochenpan/diaspora-service/actions/workflows/tests.yml/badge.svg)](https://github.com/haochenpan/diaspora-service/actions/workflows/tests.yml)
[![Deployment Status](https://github.com/haochenpan/diaspora-service/actions/workflows/deployment.yml/badge.svg)](https://github.com/haochenpan/diaspora-service/actions/workflows/deployment.yml)
[![Pre-Commit CI Status](https://results.pre-commit.ci/badge/github/haochenpan/diaspora-service/main.svg)](https://results.pre-commit.ci/latest/github/haochenpan/diaspora-service/main)
[![MIT License](https://img.shields.io/badge/license-MIT-blue)](https://opensource.org/licenses/MIT)

This repository holds the web service of the Diaspora Event Fabric. Users
sign in with Globus. For each user, the service creates an IAM user
and a namespace, the prefix of the user's Kafka topic names. It
creates the user's topics in that namespace on an Amazon MSK cluster, and
offers REST routes to manage them and to consume records from the topics.
It is a FastAPI application that runs as one container on AWS Lightsail
and keeps its records in DynamoDB.

Our goal in the [Diaspora project](https://diaspora-project.github.io/) is
to develop new methods for building next-generation resilient scientific
applications capable of executing in an increasingly integrated research
infrastructure.

## Resources

- [Documentation](https://haochenpan.github.io/diaspora-service/): how the
  service works, and how to run, test, deploy and operate it.
- [Diaspora Event SDK](https://github.com/globus-labs/diaspora-event-sdk):
  the Python SDK for the Diaspora Event Fabric.

    [![PyPI Version](https://img.shields.io/pypi/v/diaspora-event-sdk?color=teal)](https://pypi.org/project/diaspora-event-sdk/)
    [![Python Versions](https://img.shields.io/pypi/pyversions/diaspora-event-sdk.svg)](https://pypi.org/project/diaspora-event-sdk/)
    [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

- [Security policy](https://github.com/haochenpan/diaspora-service/blob/main/.github/SECURITY.md):
  how to report a vulnerability.

## Contact

For questions or contributions, contact the Diaspora Event Fabric team
through the repository's
[issue tracker](https://github.com/haochenpan/diaspora-service/issues).
Report a vulnerability privately, as the security policy says, and not in
an issue. To change the code or the docs, read the
[contributing guide](https://github.com/haochenpan/diaspora-service/blob/main/CONTRIBUTING.md).
