"""Tests of the filter on kafka-python's repeated "topic not found" errors."""

from __future__ import annotations

import logging
import uuid

import pytest

import web_service.main  # noqa: F401  (installs the filter)
from web_service.services import install_kafka_log_filters
from web_service.services import TopicNotFoundLogFilter


def _record(msg: str, *args: object) -> logging.LogRecord:
    return logging.LogRecord(
        'kafka.cluster',
        logging.ERROR,
        'cluster.py',
        591,
        msg,
        args,
        None,
    )


def test_each_topic_passes_once_per_interval() -> None:
    now = [1000.0]
    kept = TopicNotFoundLogFilter(interval_s=60, clock=lambda: now[0])
    message = TopicNotFoundLogFilter.MESSAGE
    assert kept.filter(_record(message, 'ns-a.t'))
    now[0] += 0.1
    assert not kept.filter(_record(message, 'ns-a.t'))
    assert kept.filter(_record(message, 'ns-a.other'))
    now[0] += 59.8
    assert not kept.filter(_record(message, 'ns-a.t'))
    now[0] += 0.2
    assert kept.filter(_record(message, 'ns-a.t'))


def test_other_records_pass() -> None:
    kept = TopicNotFoundLogFilter(clock=lambda: 0.0)
    for _ in range(3):
        assert kept.filter(
            _record('Topic %s is not authorized for this client', 'ns-a.t'),
        )
        assert kept.filter(_record('Metadata refresh: failed %s', 'boom'))
        assert kept.filter(_record(TopicNotFoundLogFilter.MESSAGE))


def test_the_service_filters_the_kafka_cluster_logger(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The app installs the filter once; repeats of a topic are dropped."""
    install_kafka_log_filters()
    cluster_logger = logging.getLogger('kafka.cluster')
    assert (
        sum(
            isinstance(f, TopicNotFoundLogFilter)
            for f in cluster_logger.filters
        )
        == 1
    )
    # A topic no earlier run in this process has logged
    topic = f'ns-filter-test.{uuid.uuid4().hex}'
    with caplog.at_level(logging.ERROR, logger='kafka.cluster'):
        for _ in range(5):
            cluster_logger.error(
                TopicNotFoundLogFilter.MESSAGE,
                topic,
            )
        cluster_logger.error('Topic %s is not authorized', 'ns-filter-test.x')
    assert [r.getMessage() for r in caplog.records] == [
        f'Topic {topic} not found in cluster metadata',
        'Topic ns-filter-test.x is not authorized',
    ]
