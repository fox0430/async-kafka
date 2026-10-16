## Full suite: all unit tests plus the integration tests, which require the
## real Kafka broker the development deployment provides — PLAINTEXT on
## 127.0.0.1:9092, SASL_PLAINTEXT on 127.0.0.1:9094, and SASL_SSL on localhost:9095.

{.push warning[UnusedImport]: off.}
import unit_tests, test_integration
{.pop.}
