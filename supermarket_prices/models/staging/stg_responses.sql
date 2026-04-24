-- depends_on: {{ source('dagster','warehouse_ready') }}

with 

source as (

	select * from {{ source('public', 'raw_responses') }}

),

responses as (

	select
		
		----------  ids
		raw_id,
		scrape_id,

		----------  strings
		store,

		----------  timestamps
		time as scraped_at

	from source

)

select * from responses
