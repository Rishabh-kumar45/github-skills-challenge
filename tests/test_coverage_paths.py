import json
import os
import runpy
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from aiops_pipeline import run_pipeline
from calculations import area_of_circle, get_nth_fibonacci
from anomaly_detector import AnomalyDetector
from event_producer import EventProducer
from event_topic import EventTopic


def test_calculations_reject_negative_values_and_calculate_sequence():
    with pytest.raises(ValueError, match="Radius cannot be negative"):
        area_of_circle(-1)

    with pytest.raises(ValueError, match="n cannot be negative"):
        get_nth_fibonacci(-1)

    assert get_nth_fibonacci(10) == 55


def test_detector_reports_all_supported_anomaly_reasons():
    record = {
        "timestamp": "2026-09-20T10:05:00",
        "service": "payment-service",
        "response_time_ms": 600,
        "cpu_percent": 90,
        "memory_percent": 90,
        "log_level": "WARNING",
    }

    event = AnomalyDetector().detect(record)

    assert event["reasons"] == [
        "High response time",
        "High CPU utilization",
        "High memory utilization",
        "Error log detected",
    ]


def test_empty_events_are_not_published_and_topics_can_be_cleared():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    assert producer.publish(None) is False
    producer.publish({"type": "ANOMALY"})
    assert topic.get_messages() == [{"type": "ANOMALY"}]

    topic.clear()
    assert topic.get_messages() == []


def test_pipeline_loads_records_and_collects_anomalies(tmp_path):
    data_file = tmp_path / "service_data.json"
    data_file.write_text(
        json.dumps(
            [
                {
                    "timestamp": "2026-09-20T10:00:00",
                    "service": "payment-service",
                    "response_time_ms": 120,
                    "cpu_percent": 42,
                    "memory_percent": 51,
                    "log_level": "INFO",
                },
                {
                    "timestamp": "2026-09-20T10:05:00",
                    "service": "payment-service",
                    "response_time_ms": 610,
                    "cpu_percent": 75,
                    "memory_percent": 70,
                    "log_level": "ERROR",
                },
            ]
        ),
        encoding="utf-8",
    )

    result = run_pipeline(data_file)

    assert result["records_processed"] == 2
    assert len(result["anomalies_detected"]) == 1
    assert result["events_consumed"] == []


def test_pipeline_script_prints_summary(monkeypatch):
    repository_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    monkeypatch.chdir(repository_root)

    runpy.run_path(os.path.join(repository_root, "src", "aiops_pipeline.py"), run_name="__main__")
