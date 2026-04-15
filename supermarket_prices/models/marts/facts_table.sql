{{
	config(
		materialized='incremental',
		unique_key=['product_id', 'store_id', 'date_id']
	)
}}

with

facts as (

	select
		ip.scrape_id,
		dd.date_id,
		dp.product_id,
		ds.store_id,
		ip.regular_price,
		ip.discount_price,
		ip.scraped_at
	
	from {{ ref('int_products') }} ip
	inner join {{ ref('dim_date') }} dd
		on ip.scraped_at::date = dd.date_day
	inner join {{ ref('dim_products') }} dp
		on ip.ean = dp.ean
	inner join {{ ref('dim_store') }} ds
		on ip.store = ds.store

	{% if is_incremental() %}
	where ip.scraped_at > (select max(scraped_at) from {{ this }})
	{% endif %}

)

select * from facts
