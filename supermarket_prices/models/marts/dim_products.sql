{{
	config(
		materialized='table'
	)
}}

-- We use a SCD Type 1 to describe the products dimension table

with

products as (
	
	select
		{{ dbt_utils.generate_surrogate_key(['ean']) }} as product_id,
		ean,
		product_name,
		category,
		subcategory,
		brand,
		scraped_at

	from {{ ref('int_products') }}

),

ranked_products as (

	select
		p.*,
		row_number() over (partition by product_id order by scraped_at desc) as rn

	from products p

)

select 
	product_id,
	ean,
	product_name,
	category,
	subcategory,
	brand,
	scraped_at

from ranked_products
where rn = 1
