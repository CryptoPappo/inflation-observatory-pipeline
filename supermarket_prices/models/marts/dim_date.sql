{{
    config(
        materialized = "table"
    )
}}

with

date_dimension as (

	select * from {{ ref("dates") }}
),

fiscal_periods as (

	{{
		dbt_date.get_fiscal_periods(
			ref("dates"), year_end_month=1, week_start_day=1, shift_year=1
		)
	}}
),

full_dates as (

	select 
		d.*,
		f.fiscal_week_of_year,
	    	f.fiscal_week_of_period,
	    	f.fiscal_period_number,
	    	f.fiscal_quarter_number,
	    	f.fiscal_period_of_quarter
	
	from date_dimension d
	left join fiscal_periods f on d.date_day = f.date_day
),

--- Gauss Easter Algortihm

step1 as (

	select
		full_dates.*,
		year_number % 19 as a,
		year_number % 4 as b,
		year_number % 7 as c,
		year_number / 100 as k

	from full_dates
),

step2 as (

	select 
		step1.*,
		(13 + 8*k) / 25 as p,
		k / 4 as q

	from step1
),

step3 as (

	select 
		step2.*,
		(15 - p + k - q) % 30 as M,
		(4 + k - q) % 7 as N

	from step2
),

step4 as (

	select
		step3.*,
		(19*a + M) % 30 as d

	from step3
),

step5 as (

	select
		step4.*,
		(2*b + 4*c + 6*d + N) % 7 as e

	from step4
),

step6 as (

	select
		step5.*,
		case
			when e = 6 and (d = 29 or (d = 28 and a > 10)) then 7 
			else 0 
		end as o

	from step5
),

step7 as (

	select
		step6.*,
		make_date(year_number, 03, 22) + (d + e - o) as easter_date

	from step6
)

select * from step7
