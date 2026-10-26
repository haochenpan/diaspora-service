# Security Policy

## Reporting a vulnerability

Report a vulnerability privately through
[GitHub's private vulnerability reporting](https://github.com/haochenpan/diaspora-service/security/advisories/new).
Do not open a public issue or pull request about it.

Include what you found, how to reproduce it, and what an attacker could do
with it.

## Scope

- The web service's API: the code in this repository and its running
  deployment.
- The Kafka console (the Diaspora Admin Console) and its startup script
  in `docs/kafka-console/`.

The console runs [kafbat-ui](https://github.com/kafbat/kafka-ui). Report a
vulnerability in kafbat-ui itself to that project; report here a problem in
how this deployment configures or exposes it.
