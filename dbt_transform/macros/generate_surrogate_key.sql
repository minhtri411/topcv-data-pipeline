{% macro generate_surrogate_key(field_list) -%}
    {%- set pieces = [] -%}
    {%- for field in field_list -%}
        {%- do pieces.append("coalesce(cast(" ~ field ~ " as text), '_dbt_utils_surrogate_key_null_')") -%}
    {%- endfor -%}

    md5(cast({{ pieces | join(" || '-' || ") }} as text))
{%- endmacro %}