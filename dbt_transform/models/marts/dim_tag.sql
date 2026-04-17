with tags as (
    select distinct
        tag_name_norm
    from {{ ref('int_job_tags') }}
)

select
    row_number() over (order by tag_name_norm)::integer as tag_id,
    tag_name_norm as tag_name
from tags
