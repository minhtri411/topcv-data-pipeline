with loc as (
    select distinct
        location_name_norm
    from {{ ref('int_job_locations') }}
)

select
    row_number() over (order by location_name_norm)::integer as location_id,
    location_name_norm as location_code,
    initcap(location_name_norm) as location_name
from loc
