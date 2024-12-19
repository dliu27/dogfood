from pathlib import Path

from dagster import (
    Definitions,
    ScheduleDefinition,
    SensorEvaluationContext,
    sensor,
    job,
    SkipReason,
    schedule,
    ScheduleEvaluationContext,
    RunRequest,
    define_asset_job,
    graph_asset,
    link_code_references_to_git,
    load_assets_from_package_module,
    op,
    with_source_code_references,
)
from dagster._core.definitions.metadata.source_code import AnchorBasedFilePathMapping

from . import assets

daily_refresh_schedule = ScheduleDefinition(
    job=define_asset_job(name="all_assets_job"), cron_schedule="0 0 * * *"
)


@op
def foo_op():
    return 5


@graph_asset
def my_asset():
    return foo_op()


my_assets = with_source_code_references(
    [
        my_asset,
        *load_assets_from_package_module(assets),
    ]
)

my_assets = link_code_references_to_git(
    assets_defs=my_assets,
    git_url="https://github.com/dagster-io/dagster/",
    git_branch="master",
    file_path_mapping=AnchorBasedFilePathMapping(
        local_file_anchor=Path(__file__).parent,
        file_anchor_path_in_repository="examples/quickstart_etl/quickstart_etl/",
    ),
)



@op(
    config_schema={"num": int},
)
def requires_config(context):
    return context.op_config


@job(
    tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "3"},
                },
            }
        }
    },
)
def simple_config_job():
    requires_config()


@sensor(jobs=[simple_config_job])
def test_success_sensor(context):
    cursor = context.cursor if context.cursor else 0
    context.update_cursor(str(int(cursor) + 1))
    for i in range(3):
        yield RunRequest(
            run_key=str(i),
            job_name=simple_config_job.name,
            run_config={"ops": {"requires_config": {"config": {"num": 0}}}},
            tags={"fee": "fifofum"},
        )

@sensor(jobs=[simple_config_job])
def test_skip_sensor(context):
    yield SkipReason(f"No s3 updates found for bucket.")
    return

@sensor(jobs=[simple_config_job])
def test_error_sensor(context):
    raise Exception(
        "S3 bucket not specified at environment variable `DAGSTER_TOY_SENSOR_S3_BUCKET`."
    )

@sensor(jobs=[simple_config_job])
def test_timeout_sensor(context):
    import time
    time.sleep(150)
    return
    raise Exception(
        "S3 bucket not specified at environment variable `DAGSTER_TOY_SENSOR_S3_BUCKET`."
    )

@schedule("* * * * *", job=simple_config_job)
def test_success_schedule():
    for i in range(2):
        yield RunRequest(
            job_name=simple_config_job.name,
            run_key=str(i),
            run_config={"ops": {"requires_config": {"config": {"num": 0}}}},
            tags={"fee": "fifofum"},
        )
        
@schedule("* * * * *", job=simple_config_job)
def test_exception_schedule(context: ScheduleEvaluationContext):
    tick_time = context.scheduled_execution_time  # Access the tick time
    if tick_time is not None:  # This should always be available for schedules
        tick_minute = tick_time.minute
        if tick_minute % 2 == 1:
            raise Exception(f"Exception at tick time: {tick_time}")
        return RunRequest(
            job_name=simple_config_job.name,
            run_key="1",
            run_config={"ops": {"requires_config": {"config": {"num": "1"}}}},
            tags={"fee": "fifofum"},
        )

@schedule("* * * * *", job=simple_config_job)
def test_skip_schedule(context: ScheduleEvaluationContext):
    tick_time = context.scheduled_execution_time  # Access the tick time
    if tick_time is not None:  # This should always be available for schedules
        tick_minute = tick_time.minute
        if tick_minute % 2 == 1:
            yield SkipReason("A skip reason.")
            return
        return RunRequest(
            job_name=simple_config_job.name,
            run_key="1",
            run_config={"ops": {"requires_config": {"config": {"num": "1"}}}},
            tags={"fee": "fifofum"},
        )


@schedule("* * * * *", job=simple_config_job)
def test_timeout_schedule(context: ScheduleEvaluationContext):
    tick_time = context.scheduled_execution_time  # Access the tick time
    if tick_time is not None:  # This should always be available for schedules
        tick_minute = tick_time.minute
        if tick_minute % 2 == 1:
            import time
            time.sleep(80)
            return
        return RunRequest(
            job_name=simple_config_job.name,
            run_key="1",
            run_config={"ops": {"requires_config": {"config": {"num": "1"}}}},
            tags={"fee": "fifofum"},
        )


@op
def set_up_job_run(
    context,
):
    pass


@job
def my_job():
    set_up_job_run()


@sensor(
    job=my_job,
    minimum_interval_seconds=5,
)
def process_new_event_for_social_listening(
    context: SensorEvaluationContext,
):
    yield RunRequest()

defs = Definitions(
    assets=my_assets,
    schedules=[test_success_schedule, test_exception_schedule, test_skip_schedule, test_timeout_schedule],
    sensors=[test_success_sensor, test_skip_sensor, test_error_sensor, test_timeout_sensor, process_new_event_for_social_listening]
)

