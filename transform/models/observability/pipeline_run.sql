{#- Steps of the pipeline runs that reached main. An orchestrator attempt suffixes its run
    id with #<try>; the attempts of a step fold into one row, timed from the first start to
    the last finish, with the outcome of the last attempt. -#}

with ingestions as (
    select
        regexp_replace(run_id, '#[0-9]+$', '') as pipeline_run_id,
        concat('ingest ', lower(source)) as step,
        'spark' as engine,
        run_id,
        started_at,
        finished_at,
        status,
        records_written,
        cast(null as bigint) as nodes,
        cast(null as bigint) as failed_nodes
    from {{ source('audit', 'ingestion_run') }}
),

invocations as (
    select
        regexp_replace(pipeline_run_id, '#[0-9]+$', '') as pipeline_run_id,
        concat(command, coalesce(concat(' ', selection), '')) as step,
        'dbt' as engine,
        invocation_id as run_id,
        min(run_started_at) as started_at,
        max(recorded_at) as finished_at,
        if(count_if(status in ('error', 'fail', 'runtime error')) > 0, 'FAILED', 'SUCCEEDED') as status,
        sum(if(resource_type in ('model', 'seed'), rows_affected)) as records_written,
        count(*) as nodes,
        count_if(status in ('error', 'fail', 'runtime error')) as failed_nodes
    from {{ source('audit', 'dbt_node_run') }}
    group by 1, 2, 3, 4
),

steps as (
    select * from ingestions
    union all
    select * from invocations
),

dates as (
    select regexp_replace(pipeline_run_id, '#[0-9]+$', '') as pipeline_run_id, max(business_date) as business_date
    from {{ source('audit', 'dbt_node_run') }}
    group by 1
)

select
    steps.pipeline_run_id,
    dates.business_date,
    cast(steps.step as varchar) as step,
    cast(steps.engine as varchar) as engine,
    count(*) as attempts,
    cast(min(steps.started_at) as timestamp(6)) as started_at,
    date_diff('millisecond', min(steps.started_at), max(steps.finished_at)) / 1000e0 as duration_seconds,
    cast(max_by(steps.records_written, steps.started_at) as bigint) as records_written,
    max_by(steps.nodes, steps.started_at) as nodes,
    max_by(steps.failed_nodes, steps.started_at) as failed_nodes,
    cast(max_by(steps.status, steps.started_at) as varchar) as status
from steps
left join dates on dates.pipeline_run_id = steps.pipeline_run_id
group by steps.pipeline_run_id, dates.business_date, steps.step, steps.engine
