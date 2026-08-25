with snap as (
    select
        dbt_scd_id,
        company_bk,
        company_url,
        company_name_full,
        company_website,
        company_size,
        company_followers,
        company_industry,
        company_address,
        company_description,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('snap_dim_company') }}
)

select
    ('x' || substr(md5(dbt_scd_id), 1, 16))::bit(64)::bigint as company_sk,
    dbt_scd_id,
    company_bk as external_company_id,
    company_name_full,
    company_url,
    company_website,
    company_size,
    company_followers,
    company_industry,
    company_address,
    company_description,
    dbt_valid_from,
    dbt_valid_to
from snap
