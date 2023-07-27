{% macro compare_counts(attr_labels_schema, attr_labels_table, wallet_score_schema, wallet_score_table) %}

  with field_mappings as (
    select *
    from (values
      ('crypto_natives_score', 'audience:early_adopters'),
      ('developers_score', 'audience:developers'),
      ('art_score', 'audience:culture:art'),
      ('fashion_score', 'audience:culture:fashion'),
      ('luxury_score', 'audience:culture:luxury'),
      ('music_score', 'audience:culture:music'),
      ('gaming_score', 'audience:gaming'),
      ('defi_score', 'audience:defi')
    ) as t(score_id, label)
  ),

  wallet_score_counts as (
    select
      fm.label,
      count(*) as wallet_score_count
    from {{ wallet_score_schema }}.{{ wallet_score_table }} ws
    join field_mappings fm on ws.score_id = fm.score_id
    group by fm.label
  ),

  attr_labels_counts as (
    select
      unnest(labels) as label,
      count(*) as attr_labels_count
    from {{ attr_labels_schema }}.{{ attr_labels_table }}
    group by 1
  )

  select
    wc.label,
    wc.wallet_score_count,
    alc.attr_labels_count,
    abs(wc.wallet_score_count - alc.attr_labels_count) <= 1 as counts_almost_equal
  from wallet_score_counts wc
  join attr_labels_counts alc on wc.label = alc.label
  where not counts_almost_equal

{% endmacro %}
