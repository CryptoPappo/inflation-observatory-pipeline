{{
    config(
        materialized = "table"
    )
}}

with

dates_raw as (
	
	{{ dbt_utils.date_spine(
		datepart="day",
		start_date="cast('2000-01-01' as date)",
		end_date="date_add(current_date(), interval 20 year)"
		)
	}}

)

select * from dates_raw
