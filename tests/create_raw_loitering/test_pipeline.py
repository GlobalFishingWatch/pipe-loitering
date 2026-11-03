import json

from types import SimpleNamespace
from unittest import mock

import pytest

from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners import PipelineState

from pipe_loitering.create_raw_loitering import pipeline as raw_pipeline


LABELS = {"environment": "test", "stage": "loitering"}


@pytest.fixture
def options():
    return SimpleNamespace(
        bq_input="project.dataset.messages",
        bq_output="project.dataset.raw_loitering",
        date_range=("2024-01-01", "2024-01-02"),
        slow_threshold=2,
        source_timestamp_field="timestamp",
        labels=LABELS,
        unknown_unparsed_args=(),
        unknown_parsed_args={"project": "project"},
    )


def test_dataflow_job_is_labelled(options):
    loitering = raw_pipeline.LoiteringPipeline(options)

    labels = loitering.pipeline.options.view_as(GoogleCloudOptions).labels

    # Beam forwards the dict as one JSON flag, which the Dataflow runner parses.
    assert [json.loads(label) for label in labels] == [LABELS]


def test_delete_query_is_labelled_and_awaited(options):
    loitering = raw_pipeline.LoiteringPipeline(options)
    loitering.pipeline = mock.Mock()
    loitering.pipeline.run.return_value.state = PipelineState.FAILED

    with mock.patch.object(raw_pipeline.bigquery, "Client") as client:
        loitering.run()

    query = client.return_value.query
    assert query.call_args.kwargs["job_config"].labels == LABELS
    query.return_value.result.assert_called_once()
