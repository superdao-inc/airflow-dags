{{
    config(
        materialised = "table",
        engine = 'MergeTree()',
    )
}}
select 1 as col
