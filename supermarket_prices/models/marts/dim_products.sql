{{
	config(
		materialized='table'
	)
}}

-- We use a SCD Type 1 to describe the products dimension table

with

ranked_products as (
	
	select
		product_id,
		product_name,
		category,
		subcategory,
		brand,
		scraped_at,
		row_number() over (partition by product_id order by scraped_at desc) as rn

	from {{ ref('int_products') }}

)

select 
	product_id,
	product_name,
	category,
	subcategory,
	brand,
	scraped_at

from ranked_products
where rn = 1
