import sys
import time

import pytest

from dagster import (
    DagsterEventType,
    DagsterInvariantViolationError,
    ExpectationResult,
    PipelineDefinition,
    Result,
    execute_pipeline,
    solid,
)


@pytest.mark.skipif(
    sys.platform == 'win32', reason='https://github.com/dagster-io/dagster/issues/1421'
)
def test_event_timing_before_yield():
    @solid
    def before_yield_solid(_context):
        time.sleep(0.01)
        yield Result(None)

    pipeline_def = PipelineDefinition(solids=[before_yield_solid])
    pipeline_result = execute_pipeline(pipeline_def)
    success_event = pipeline_result.result_for_solid('before_yield_solid').get_step_success_event()
    assert success_event.event_specific_data.duration_ms >= 10.0


@pytest.mark.skipif(
    sys.platform == 'win32', reason='https://github.com/dagster-io/dagster/issues/1421'
)
def test_event_timing_after_yield():
    @solid
    def after_yield_solid(_context):
        yield Result(None)
        time.sleep(0.01)

    pipeline_def = PipelineDefinition(solids=[after_yield_solid])
    pipeline_result = execute_pipeline(pipeline_def)
    success_event = pipeline_result.result_for_solid('after_yield_solid').get_step_success_event()
    assert success_event.event_specific_data.duration_ms >= 10.0


@pytest.mark.skipif(
    sys.platform == 'win32', reason='https://github.com/dagster-io/dagster/issues/1421'
)
def test_event_timing_direct_return():
    @solid
    def direct_return_solid(_context):
        time.sleep(0.01)
        return None

    pipeline_def = PipelineDefinition(solids=[direct_return_solid])
    pipeline_result = execute_pipeline(pipeline_def)
    success_event = pipeline_result.result_for_solid('direct_return_solid').get_step_success_event()
    assert success_event.event_specific_data.duration_ms >= 10.0


def expectation_results_for_solid_transform(result, solid_name):
    solid_result = result.result_for_solid(solid_name)
    return [
        t_event
        for t_event in solid_result.transforms
        if t_event.event_type == DagsterEventType.STEP_EXPECTATION_RESULT
    ]


def test_successful_expectation_in_transform():
    @solid(outputs=[])
    def success_expectation_solid(_context):
        yield ExpectationResult(success=True, message='This is always true.')

    pipeline_def = PipelineDefinition(
        name='success_expectation_in_transform_pipeline', solids=[success_expectation_solid]
    )

    result = execute_pipeline(pipeline_def)

    assert result
    assert result.success

    expt_results = expectation_results_for_solid_transform(result, 'success_expectation_solid')

    assert len(expt_results) == 1
    expt_result = expt_results[0]
    assert expt_result.event_specific_data.expectation_result.success
    assert expt_result.event_specific_data.expectation_result.message == 'This is always true.'


def test_failed_expectation_in_transform():
    @solid(outputs=[])
    def failure_expectation_solid(_context):
        yield ExpectationResult(success=False, message='This is always false.')

    pipeline_def = PipelineDefinition(
        name='failure_expectation_in_transform_pipeline', solids=[failure_expectation_solid]
    )

    result = execute_pipeline(pipeline_def)

    assert result
    assert result.success
    expt_results = expectation_results_for_solid_transform(result, 'failure_expectation_solid')

    assert len(expt_results) == 1
    expt_result = expt_results[0]
    assert not expt_result.event_specific_data.expectation_result.success
    assert expt_result.event_specific_data.expectation_result.message == 'This is always false.'


def test_return_expectation_failure():
    @solid(outputs=[])
    def return_expectation_failure(_context):
        return ExpectationResult(success=True, message='This is always true.')

    pipeline_def = PipelineDefinition(
        name='success_expectation_in_transform_pipeline', solids=[return_expectation_failure]
    )

    with pytest.raises(DagsterInvariantViolationError) as exc_info:
        execute_pipeline(pipeline_def)

    assert str(exc_info.value) == (
        'Error in solid return_expectation_failure: If you are returning '
        'a Materialization or an ExpectationResult from solid you must yield '
        'them to avoid ambiguity with an implied result from returning a value.'
    )
