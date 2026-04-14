{{
	config(
		materialized='table'
	)
}}

with

ranked_products as (
	
	select
		product_id,
		product_name,
		category,
		subcategory,

