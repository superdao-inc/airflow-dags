{{
  config(
    materialized = "table",
    engine = 'TinyLog()',
    tags = ['report', 'report_tokens']
  )
}}

WITH
base_table AS (
    SELECT *
    FROM VALUES('name String, eth_address Nullable(String), polygon_address Nullable(String)',
        ('USDT', '0xdac17f958d2ee523a2206206994597c13d831ec7', '0xc2132d05d31c914a87c6611c10748aeb04b58e8f'),
        ('BNB', '0xb8c77482e45f1f44de1745f52c74426c631bdd52', '0x3ba4c387f786bfee076a58914f5bd38d668b42c3'),
        ('USDC', '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', '0x2791bca1f2de4661ed88a30c99a7a9449aa84174'),
        ('BUSD', '0x4fabb145d64652a948d72533023f6e7a623c7c53', '0xdab529f40e671a1d4bf91361c21bf9f0c9712ab7'),
        ('stETH', '0xae7ab96520de3a18e5e111b5eaab095312d7fe84', '0x4c6b65fe93fc9daf413498b88195fafff36dd960'),
        ('MATIC', '0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0', '0x0000000000000000000000000000000000001010'),
        ('THETA', '0x3883f5e181fccaf8410fa61e12b59bad963fb645', '0xb46e0ae620efd98516f49bb00263317096c114b2'),
        ('SHIB', '0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce', '0x6f8a06447ff6fcf75d803135a7de15ce88c1d4ec'),
        ('DAI', '0x6b175474e89094c44da98b954eedeac495271d0f', '0x8f3cf7ad23cd3cadbd9735aff958023239c6a063'),
        ('WBTC', '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599', '0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6'),
        ('UNI', '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984', '0xb33eaad8d922b1083446dc23f610c2567fb5180f'),
        ('LINK', '0x514910771af9ca656af840dff83e8264ecf986ca', '0x53e0bca35ec356bd5dddfebbd1fc0fd03fabad39'),
        ('LEO', '0x2af5d2ad76741191d15dfe7bf6ac92d4bd912ca3', '0x06d02e9d62a13fc76bb229373fb3bbbd1101d2fc'),
        ('OKB', '0x75231f58b43240c9718dd58b4967c5114342a86c', null),
        ('TONCOIN', '0x582d872a1b094fc48f5de31d3b73f2d9be47def1', '0xd32049cf210088b65b421e09ad2e3aba71eb5d19'),
        ('HEX', '0x2b591e99afe9f32eaa6214f7b7629768c40eeb39', '0x23d29d30e35c5e8d321e1dc9a8a61bfd846d4c5c'),
        ('TUSD', '0x0000000000085d4780b73119b644ae5ecd22b376', '0x2e1ad108ff1d8c782fcbbb89aad783ac49586756'),
        ('LDO', '0x5a98fcbea516cf06857215779fd812ca3bef1b32', '0xc3c7d422809852031b44ab29eec9f1eff2a58756'),
        ('QNT', '0x4a220e6096b25eadb88358cb44068a3248254675', '0x36b77a184be8ee56f5e81c56727b20647a42e28e'),
        ('CRO', '0xa0b73e1ff0b80914ab6fe0444e65848c4c34450b', '0xada58df0f643d959c2a47c9d4d4c1a4defe3f11c'),
        ('ARB', '0xb50721bcf8d664c30412cfbc6cf7a15145234ad1', '0xceb07295e07cf9f1cb368b554ea9dc1cc6e06fe4'),
        ('VEN', '0xd850942ef8811f2a866692a623011bde52a462c1', null),
        ('NEAR', '0x85f17cf997934a597031b2e18a9ab6ebd4b9f6a4', '0x72bd80445b0db58ebe3e8db056529d4c5faf6f2f'),
        ('GRT', '0xc944e90c64b2c07662a292be6244bdf05cda44a7', '0x5fe2b58c013d7601147dcdd68c143a77499f5531'),
        ('APE', '0x4d224452801aced8b2f0aebe155379bb5d594381', '0xb7b31a6bc18e48888545ce79e83e06003be70930')
    ) t
),

eth_target_contracts AS (
    SELECT eth_address
    FROM base_table
    WHERE eth_address IS NOT NULL
),
polygon_target_contracts AS (
    SELECT polygon_address
    FROM base_table
    WHERE polygon_address IS NOT NULL
),

eth_incomes AS (
    SELECT
        token_address,
        to_address AS address,
        toInt256(sum(value)) AS income
    FROM {{ source('eth_data', 'token_transfers_by_token') }}
    WHERE token_address IN eth_target_contracts
        AND to_address NOT IN ( SELECT c.address FROM {{ source('eth_data', 'contracts_by_address') }} c )
    GROUP BY token_address, address
),
eth_outcomes AS (
    SELECT token_address,
        from_address AS address,
        toInt256(sum(value)) AS outcome
    FROM {{ source('eth_data', 'token_transfers_by_token') }}
    WHERE token_address IN eth_target_contracts
        AND from_address NOT IN ( SELECT c.address FROM {{ source('eth_data', 'contracts_by_address') }} c )
    GROUP BY token_address, address
),
eth_holders AS (
    SELECT
        i.address AS address,
        i.token_address AS token_address,
        (income - coalesce(outcome, 0)) AS balance
    FROM eth_incomes i
    LEFT JOIN eth_outcomes o ON i.address = o.address
    WHERE balance > 0
),

polygon_incomes AS (
    SELECT
        token_address,
        to_address AS address,
        toInt256(sum(value)) AS income
    FROM {{ source('polygon_data', 'token_transfers_by_token') }}
    WHERE token_address IN polygon_target_contracts
        AND to_address NOT IN ( SELECT c.address FROM {{ source('polygon_data', 'contracts_by_address') }} c )
    GROUP BY token_address, address
),
polygon_outcomes AS (
    SELECT token_address,
        from_address AS address,
        toInt256(sum(value)) AS outcome
    FROM {{ source('polygon_data', 'token_transfers_by_token') }}
    WHERE token_address IN polygon_target_contracts
        AND from_address NOT IN ( SELECT c.address FROM {{ source('polygon_data', 'contracts_by_address') }} c )
    GROUP BY token_address, address
),
polygon_holders AS (
    SELECT
        i.address AS address,
        i.token_address AS token_address,
        (income - coalesce(outcome, 0)) AS balance
    FROM polygon_incomes i
    LEFT JOIN polygon_outcomes o ON i.address = o.address
    WHERE balance > 0
),

union_holders AS (
    SELECT
        address,
        token_address
    FROM eth_holders

    UNION DISTINCT

    SELECT
        address,
        token_address
    FROM polygon_holders
)

SELECT
    name,
    uniqExact(address) AS unique_holders
FROM union_holders
LEFT JOIN base_table ON base_table.eth_address = union_holders.token_address OR base_table.polygon_address = union_holders.token_address
GROUP BY name
ORDER BY unique_holders DESC
