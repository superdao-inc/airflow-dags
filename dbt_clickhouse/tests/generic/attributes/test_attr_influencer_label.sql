{%test test_attr_influencer_label(
        model, column_name, compare_with_table, compare_with_column
    ) %}

select
    (
        select count(*) from {{model}}
        where has({{column_name}}, 'influencer')
    ) as influencer_count,

    (
        select count(*) from {{compare_with_table}}
        where toInt32(twitter_followers_count) > 1000
    ) as twitter_count

where influencer_count > twitter_count
    AND influencer_count < (twitter_count * 0.9)

{% endtest %}