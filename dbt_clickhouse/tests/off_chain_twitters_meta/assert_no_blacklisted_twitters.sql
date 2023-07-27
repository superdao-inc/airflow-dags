select
    twitter_username
from {{ ref('off_chain_twitters_meta') }}
where twitter_username in (
    'elonmusk',
    'Fendi',
    'SenWarren',
    'cz_binance',
    'SnoopDogg',
    'BebeRexha',
    'wef',
    'thetanmay',
    'DONJAZZY',
    'SBF_FTX',
    'NBA',
    'manuginobili',
    'jul',
    'YG',
    'bjnovak',
    'NVTcommunity',
    'SuiEcosystem'
)