{% macro generate_audience_label_query(chain, audience_slug) -%}

SELECT
    address

FROM {{ ref('all_audiences') }}
WHERE audience_type = 'AUDIENCE'
AND lower(chain) = lower('{{ chain }}')
AND audience_slug == '{{ audience_slug }}'

{% endmacro %}