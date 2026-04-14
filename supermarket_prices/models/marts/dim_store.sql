{{
	config(
		materialized='table'
	)
}}

with

stores as (

	select distinct
		{{ dbt_utils.generate_surrogate_key(['store']) }} as store_id,
		store

	from {{ ref('stg_responses') }}

)

select * from stores
