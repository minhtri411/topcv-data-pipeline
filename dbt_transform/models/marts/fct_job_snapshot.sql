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
        external_job_id
    from {{ ref('dim_job') }}
    where dbt_valid_to is null
),

company_dim as (
    select
        company_sk,
        company_id
    from {{ ref('dim_company') }}
    where dbt_valid_to is null
),

event_location as (
    select
        l.job_bk,
        l.snapshot_ts,
        min(l.location_name_norm) as location_name_norm
    from {{ ref('int_job_locations') }} l
    group by 1, 2
),

base as (
    select
        e.job_bk,
        e.snapshot_ts,
        e.snapshot_date_id as date_id,
        cast(to_char(e.deadline_date, 'YYYYMMDD') as integer) as deadline_date_id,
        jd.job_sk,
        cd.company_sk,
        dl.location_id,
        e.salary_min,
        e.salary_max,
        e.salary_avg,
        e.salary_currency,
        e.snapshot_hash
    from events e
    left join job_dim jd
        on e.job_bk = jd.external_job_id
    left join company_dim cd
        on e.company_bk = cd.company_id
    left join event_location el
        on e.job_bk = el.job_bk
       and e.snapshot_ts = el.snapshot_ts
    left join {{ ref('dim_location') }} dl
        on el.location_name_norm = dl.location_code
)

select
    job_bk,
    snapshot_ts,
    job_sk,
    date_id,
    company_sk,
    location_id,
    deadline_date_id,
    salary_min,
    salary_max,
    salary_avg,
    salary_currency,
    snapshot_hash
from base
