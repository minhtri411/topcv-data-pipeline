with source as (
    select *
    from {{ source('topcv_raw', 'topcv_jobs') }}
    where nullif(trim(job_url), '') is not null
),

normalized as (
    select
        cast(crawled_at as timestamp) as snapshot_ts,
        lower(trim(job_url)) as job_url,
        nullif(trim(title), '') as title,
        lower(nullif(trim(company_url), '')) as company_url,
        lower(nullif(trim(company_name_full), '')) as company_name_full,
        lower(nullif(trim(company_website), '')) as company_website,
        nullif(trim(company_size), '') as company_size,
        nullif(trim(company_followers), '') as company_followers,
        nullif(trim(company_industry), '') as company_industry,
        nullif(trim(company_address), '') as company_address,
        nullif(trim(company_description), '') as company_description,
        nullif(trim(location), '') as location_raw,
        nullif(trim(experience), '') as experience_raw,
        nullif(trim(deadline), '') as deadline_raw,
        nullif(trim(tags), '') as tags_raw,
        nullif(trim(desc_mota), '') as desc_mota,
        nullif(trim(desc_yeucau), '') as desc_yeucau,
        nullif(trim(desc_quyenloi), '') as desc_quyenloi,
        nullif(trim(working_addresses), '') as working_addresses_raw,
        nullif(trim(working_times), '') as working_times,
        lower(coalesce(salary, '')) as salary_text,
        case
            when lower(coalesce(salary, '')) like '%usd%' or coalesce(salary, '') like '%$%' then 'USD'
            else 'VND'
        end as salary_currency,
        case
            when lower(coalesce(salary, '')) like '%triệu%' or lower(coalesce(salary, '')) like '%tr%' then 1000000::numeric
            when lower(coalesce(salary, '')) like '%k%' then 1000::numeric
            else 1::numeric
        end as salary_unit_multiplier
    from source
),

salary_parsed as (
    select
        *,
        case
            when salary_text = '' or salary_text like '%thỏa thuận%' or salary_text like '%thu nhap canh tranh%' then null::numeric
            when salary_text like '%-%' then nullif(replace(regexp_replace(split_part(salary_text, '-', 1), '[^0-9\\.,]', '', 'g'), ',', ''), '')::numeric
            when salary_text like '%từ%' then nullif(replace(regexp_replace(salary_text, '[^0-9\\.,]', '', 'g'), ',', ''), '')::numeric
            when salary_text like '%trên%' then nullif(replace(regexp_replace(salary_text, '[^0-9\\.,]', '', 'g'), ',', ''), '')::numeric
            when salary_text like '%tới%' then 0::numeric
            else nullif(replace(regexp_replace(salary_text, '[^0-9\\.,]', '', 'g'), ',', ''), '')::numeric
        end as salary_min_number,
        case
            when salary_text = '' or salary_text like '%thỏa thuận%' or salary_text like '%thu nhap canh tranh%' then null::numeric
            when salary_text like '%-%' then nullif(replace(regexp_replace(split_part(salary_text, '-', 2), '[^0-9\\.,]', '', 'g'), ',', ''), '')::numeric
            when salary_text like '%tới%' then nullif(replace(regexp_replace(salary_text, '[^0-9\\.,]', '', 'g'), ',', ''), '')::numeric
            when salary_text like '%trên%' then null::numeric
            else nullif(replace(regexp_replace(salary_text, '[^0-9\\.,]', '', 'g'), ',', ''), '')::numeric
        end as salary_max_number
    from normalized
),

typed as (
    select
        {{ generate_surrogate_key(['job_url']) }} as job_bk,
        {{ generate_surrogate_key([
            "coalesce(company_url, '')",
            "coalesce(company_name_full, '')",
            "coalesce(company_website, '')"
        ]) }} as company_bk,
        1::integer as source_id,
        snapshot_ts,
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
        cast(
            case when salary_currency = 'USD' then salary_min_number * salary_unit_multiplier * 26000
                 else salary_min_number * salary_unit_multiplier end as numeric(14,2)
        ) as salary_min,
        cast(
            case when salary_currency = 'USD' then salary_max_number * salary_unit_multiplier * 26000
                 else salary_max_number * salary_unit_multiplier end as numeric(14,2)
        ) as salary_max
    from salary_parsed
),

deduped as (
    select
        *,
        row_number() over (
            partition by job_bk, snapshot_ts
            order by
                case when title is null then 1 else 0 end,
                case when company_url is null then 1 else 0 end,
                snapshot_ts desc
        ) as rn
    from typed
)

select
    job_bk,
    company_bk,
    source_id,
    snapshot_ts,
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
    salary_max
from deduped
where rn = 1
