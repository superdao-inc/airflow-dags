{% test row_count(model, column_name, above, below) %}

select row_count
from (
    select count( {{ column_name }} ) as row_count
    from {{ model }}
) tt
where false

{% if above or above == 0 %}
    OR row_count <= {{ above }}
{% endif %}

{% if below %}
    OR row_count >= {{ below }}
{% endif %}

{% endtest %}