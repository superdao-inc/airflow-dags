{{
    config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = 'address',
        tags = ['attr_labels_polygon', 'attr_labels_polygon_audience']
    )
}}

{{
    generate_audience_label_query('polygon', 'professional_score')
}}