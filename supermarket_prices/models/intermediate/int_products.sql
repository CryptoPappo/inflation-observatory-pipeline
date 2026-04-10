with

products as (

	select * from {{ ref('stg_products') }}

),

responses as (

	select * from {{ ref('stg_responses') }}

),

joined as (

	select

		----------  products 
		products.scrape_id,
		products.product_id,
		products.product_name,
		products.category_path,
		products.discount_type,
		products.regular_price,
		products.discount_price,
		
		-- check for discount
		case
			when products.discount_price <> 0 
				and products.discount_price < products.regular_price
				then true
			else false
		end as has_discount,
		
		-- category_level_1 → category_level_10
		{% for category_level in range(1, 11) %}
		split_part(products.category_path, '/', category_level) as category_level_{{category_level}},
		{% endfor %}

		----------  responses
		responses.store,
		responses.scraped_at

	from products

	inner join responses
		on products.scrape_id = responses.scrape_id

)

select * from joined
