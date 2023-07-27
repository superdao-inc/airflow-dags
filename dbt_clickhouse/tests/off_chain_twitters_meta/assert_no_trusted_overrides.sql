with
('hardcoded', 'opensea') as trusted_sources,
trusted_twitters_query as (
    select
        distinct twitter_username
    from {{ ref('off_chain_twitters_meta') }}
    where source in trusted_sources
)
select
    twitter_username
from {{ ref('off_chain_twitters_meta') }}
where twitter_username in trusted_twitters_query
    and source not in trusted_sources
