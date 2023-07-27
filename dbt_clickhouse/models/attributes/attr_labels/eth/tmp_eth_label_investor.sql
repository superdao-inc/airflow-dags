{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address',
        tags = ['attr_labels_eth', 'attr_labels_eth_audience']
    )
}}

{{
    generate_audience_label_query('ethereum', 'investor_score')
}}