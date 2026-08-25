with tags as (
    select distinct
        tag_name_norm
    from {{ ref('int_job_tags') }}
)

select
    ('x' || substr(md5(tag_name_norm), 1, 16))::bit(64)::bigint as tag_id,
    tag_name_norm as tag_name
from tags
