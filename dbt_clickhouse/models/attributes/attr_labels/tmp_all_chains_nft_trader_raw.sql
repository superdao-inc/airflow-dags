{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address',
        tags = ['attr_labels_prerequisites', 'label_nft_trader']
    )
}}

with

buyers as (
  select address from (
    select address from {{ ref('eth_ntf_bought__3_months') }}
    union all
    select address from {{ ref('polygon_ntf_bought__3_months') }}
  ) group by address having count() >= 5
),

sellers as (
  select address from (
    select address from {{ ref('eth_ntf_sell__3_months') }}
    union all
    select address from {{ ref('polygon_ntf_sell__3_months') }}
  ) group by address having count() >= 5
)

select distinct address from (
    select address from buyers
    union all
    select address from sellers
)