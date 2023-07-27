{{
   config(
        materialized = "incremental",
        engine = 'MergeTree()',
        order_by = '(base_audience, holded_contract)',
        incremental_strategy = 'append',
        tags = ['chunks']
   )
}}

{% set token_buckets = var("token_buckets", []) %}

select 
    base_audience
    , holded_contract
    , holders
    , nft_count
    , rn
from (
  select
      base_audience
      , holded_contract
      , holders
      , nft_count
      , row_number() over (partition by base_audience order by holders desc) as rn
  from (
    select 
      e1.token_address as base_audience
      , a.token_address as holded_contract
      , count(distinct wallet) as holders
      , sum(token_qty) as nft_count
    from {{ ref('eth_wallet_tokens_without_contracts_by_token') }} e1
    join (
      select distinct a.token_address, wallet
      from  {{ ref('eth_wallet_tokens_without_contracts') }} a
      where 1=1
    ) a  on a.wallet = e1.wallet
    where 1=1
    {% if token_buckets %}
      and (
        {% for bucket in token_buckets %}
          startsWith(e1.token_address, '{{ bucket }}')
          {% if not loop.last %} or {% endif %}
        {% endfor %}
      )
    {% endif %}
    group by base_audience, holded_contract
  ) t
) tt
where rn <= 25