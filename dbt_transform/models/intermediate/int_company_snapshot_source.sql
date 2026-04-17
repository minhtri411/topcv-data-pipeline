with ranked as (
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
        snapshot_ts,
        row_number() over (
            partition by company_bk
            order by snapshot_ts desc
        ) as rn
    from {{ ref('stg_topcv_jobs') }}
    where company_bk is not null
)

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
    snapshot_ts as updated_at
from ranked
where rn = 1
