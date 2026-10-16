## Aggregates every unit-test module (everything that runs without an
## external Kafka broker). The integration entry point adds the integration
## suite on top; keep the module list here so it exists in exactly one place.

{.push warning[UnusedImport]: off.}
import
  test_assignment, test_cluster, test_consumer, test_connection, test_errors,
  test_facade, test_fuzz, test_group_metadata, test_internal, test_fake_broker,
  test_fetcher, test_producer, test_producer_metrics, test_protocol, test_retry,
  test_sasl, test_partitioner, test_schema_gen, test_state_machine, test_throttle,
  test_public_api_docs, test_duplicate_blocks, test_metric_events, test_cancel_policy,
  test_broker_encapsulation, test_wire_spec_drift
{.pop.}
