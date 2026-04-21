{% snapshot snap_dim_company %}

{{
    config(
      target_schema='snapshots',
      unique_key='company_bk',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True
    )
}}

select
    company_bk,
    company_url,
    company_name_full,
    company_website,
    company_size,
    company_followers,
    company_industry,
    company_address,
    company_description,
    updated_at
from {{ ref('int_company_snapshot_source') }}

{% endsnapshot %}
