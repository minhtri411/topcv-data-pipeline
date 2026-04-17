with src as (
    select * from {{ ref('stg_topcv_jobs') }}
),

shaped as (
    select
        job_bk,
        company_bk,
        source_id,
        snapshot_ts,
        cast(to_char(snapshot_ts::date, 'YYYYMMDD') as integer) as snapshot_date_id,
        job_url,
        title,
        company_url,
        company_name_full,
        company_website,
        company_size,
        company_followers,
        company_industry,
        company_address,
        company_description,
        location_raw,
        experience_raw,
        deadline_raw,
        tags_raw,
        desc_mota,
        desc_yeucau,
        desc_quyenloi,
        working_addresses_raw,
        working_times,
        salary_currency,
        salary_min,
        salary_max,
        case
            when deadline_raw ~ '^\\d{1,2}/\\d{1,2}/\\d{4}$' then to_date(deadline_raw, 'DD/MM/YYYY')
            else null::date
        end as deadline_date,
        case
            when salary_min is not null and salary_max is not null then cast((salary_min + salary_max) / 2 as numeric(14,2))
            else coalesce(salary_min, salary_max)
        end as salary_avg,
        {{ generate_surrogate_key([
            'job_bk',
            'snapshot_ts',
            'title',
            'company_bk',
            'location_raw',
            'tags_raw',
            'salary_min',
            'salary_max'
        ]) }} as snapshot_hash
    from src
)

select * from shaped
