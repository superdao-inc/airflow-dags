{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address',
        tags = ['attr_labels_prerequisites', 'label_nft_trader']
    )
}}

with

contracts as (
  select address from {{ ref('eth_contracts_by_address') }}
  union all
  select address from {{ ref('polygon_contracts_by_address') }}
)

select address from {{ ref('tmp_all_chains_nft_trader_raw') }}
where address not in contracts