{{
  config(
    materialized='incremental',
    unique_key=['job_bk', 'snapshot_ts'],
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns'
  )
}}

with events as (
    select *
    from {{ ref('int_job_snapshot_events') }}

    {% if is_incremental() %}
      where snapshot_ts >= (
          select coalesce(max(snapshot_ts), '1900-01-01'::timestamp)
          from {{ this }}
      )
    {% endif %}
),

job_dim as (
    select
        job_sk,
        external_job_id,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('dim_job') }}
),

company_dim as (
    select
        company_sk,
        external_company_id,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('dim_company') }}
),

base as (
    select
        e.job_bk,
        e.snapshot_ts,
        e.snapshot_date_id as date_id,
        cast(to_char(e.deadline_date, 'YYYYMMDD') as integer) as deadline_date_id,
        jd.job_sk,
        cd.company_sk,
        e.company_bk,
        e.salary_min,
        e.salary_max,
        e.salary_avg,
        e.salary_currency,
        e.snapshot_hash
    from events e

    left join job_dim jd
        on e.job_bk = jd.external_job_id
       and e.snapshot_ts >= jd.dbt_valid_from
       and e.snapshot_ts < coalesce(
           jd.dbt_valid_to,
           'infinity'::timestamp
       )

    left join company_dim cd
        on e.company_bk = cd.external_company_id
       and e.snapshot_ts >= cd.dbt_valid_from
       and e.snapshot_ts < coalesce(
           cd.dbt_valid_to,
           'infinity'::timestamp
       )
)


select
    job_bk,
    snapshot_ts,
    job_sk,
    date_id,
    company_sk,
    company_bk,
    deadline_date_id,
    salary_min,
    salary_max,
    salary_avg,
    salary_currency,
    snapshot_hash
from base