with 

source as (

	select
		scrape_id,
		normalized_payload as payload

	from {{ source('raw', 'normalized_responses') }}

),

products as (
	
	select
		
		----------  ids
		scrape_id,
		(payload ->> 'ean')::bigint as product_id,

		----------  strings
		payload ->> 'name' as product_name,
		payload ->> 'category' as category,
		payload ->> 'subcategory' as subcategory,
		payload ->> 'discount' as discount_type,

		----------  numerics
		(payload ->> 'regular_price')::numeric as regular_price,
		(payload ->> 'discount_price')::numeric as discount_price
	
	from source

)

select * from products
