{{
    config(
        materialized='ephemeral'
    )
}}

select 'BTCUSDT' as binance_symbol, 'btc' aS coin_id
union distinct
select 'LTCUSDT' as binance_symbol, 'ltc' aS coin_id
union distinct
select 'ETHUSDT' as binance_symbol, 'eth' aS coin_id
union distinct
select 'BUSDUSDT' as binance_symbol, 'busd' aS coin_id
union distinct
select 'DAIUSDT' as binance_symbol, 'dai' aS coin_id
union distinct
select 'USDCUSDT' as binance_symbol, 'usdc' aS coin_id
