{% test test_all_section_of_numbers(model, column_name) %}

select count(distinct {{column_name}})::float / count(1)::float as ratio
from {{model}}
having ratio <> 1

{% endtest %}