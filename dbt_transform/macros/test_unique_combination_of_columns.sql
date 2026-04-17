{% test unique_combination_of_columns(model, combination_of_columns) %}
with grouped as (
    select
        {{ combination_of_columns | join(', ') }},
        count(*) as duplicate_count
    from {{ model }}
    group by {{ combination_of_columns | join(', ') }}
    having count(*) > 1
)

select *
from grouped
{% endtest %}