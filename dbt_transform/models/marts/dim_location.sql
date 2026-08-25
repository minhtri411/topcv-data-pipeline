with loc as (
    select distinct
        location_name_norm
    from {{ ref('int_job_locations') }}
)

select
    ('x' || substr(md5(location_name_norm), 1, 16))::bit(64)::bigint as location_id,
    location_name_norm as location_code,
    initcap(location_name_norm) as location_name
from loc
