{% snapshot snap_dim_job %}

{{
    config(
      target_schema='mart',
      unique_key='job_bk',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True
    )
}}

select
    job_bk,
    source_id,
    title,
    job_url,
    experience_raw,
    desc_mota,
    desc_yeucau,
    desc_quyenloi,
    working_addresses_raw,
    working_times,
    updated_at
from {{ ref('int_job_snapshot_source') }}

{% endsnapshot %}
