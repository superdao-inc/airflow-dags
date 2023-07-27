


select 'eth' as chain, 'dai' as token_name
    , '0x6b175474e89094c44da98b954eedeac495271d0f' as token_contract
    , 1e18 as decimals

union all
select 'eth' as chain, 'usdc' as token_name
    , '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' as token_contract
    , 1e6

union all
select 'eth' as chain, 'usdt' as token_name
    , '0xdac17f958d2ee523a2206206994597c13d831ec7' as token_contract
    , 1e6

union all
select 'polygon' as chain, 'dai' as token_name
    , '0x8f3cf7ad23cd3cadbd9735aff958023239c6a063' as token_contract
    , 1e18 as decimals

union all
select 'polygon' as chain, 'usdc' as token_name
    , '0x2791bca1f2de4661ed88a30c99a7a9449aa84174' as token_contract
    , 1e6

union all
select 'polygon' as chain, 'usdt' as token_name
    , '0xc2132d05d31c914a87c6611c10748aeb04b58e8f' as token_contract
    , 1e6