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
		products.raw_id,
		products.ean,
		products.product_name,
		products.category,
		products.subcategory,
		products.brand,
		products.discount_type,
		products.regular_price,
		products.discount_price,

		----------  responses
		resonses.scrape_id,
		responses.store,
		responses.scraped_at

	from products

	inner join responses
		on products.raw_id = responses.raw_id

)

select * from joined
