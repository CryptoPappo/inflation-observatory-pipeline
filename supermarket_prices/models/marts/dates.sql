{{
    config(
        materialized = "table"
    )
}}

{{ dbt_date.get_date_dimension("2000-01-01", "2050-01-01") }}
