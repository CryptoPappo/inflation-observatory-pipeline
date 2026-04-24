-- depends_on: {{ source('dagster','warehouse_ready') }}

with 

source as (

	select
		raw_id,
		normalized_payload as payload

	from {{ source('public', 'normalized_responses') }}

),

products as (
	
	select
		
		----------  ids
		raw_id,
		(payload ->> 'ean')::bigint as ean,

		----------  strings
		payload ->> 'name' as product_name,
		payload ->> 'category' as category,
		payload ->> 'subcategory' as subcategory,
		payload ->> 'brand' as brand,
		payload ->> 'discount' as discount_type,

		----------  numerics
		regexp_replace(payload ->> 'regular_price', '[^0-9.]', '', 'g')::numeric as regular_price,
		regexp_replace(payload ->> 'discount_price', '[^0-9.]', '', 'g')::numeric as discount_price
	
	from source

)

select * from products
