{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address'
    )
}}

select 
    lower(address) as address
    , min(email) as email
    , now() as updated
from {{source('off_chain', 'superdao_users')}} se
where se.email is not null and se.email <> ''
group by address