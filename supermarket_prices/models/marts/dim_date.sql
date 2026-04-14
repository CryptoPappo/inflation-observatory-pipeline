{{
    config(
        materialized = 'table',
	unique_key='date_key'
    )
}}

with

date_dimension as (

	select * from {{ ref('dates') }}
),

fiscal_periods as (

	{{
		dbt_date.get_fiscal_periods(
			ref('dates'), year_end_month=1, week_start_day=1, shift_year=1
		)
	}}
),

full_dates as (

	select 
		d.*,
		to_char(d.date_day, 'MM-DD') as month_day,
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
),

full_holidays as (

	select
		to_char(date_day, 'YYYYMMDD') as date_key,
		date_day,
		month_day,
		prior_date_day,
		next_date_day,
		prior_year_date_day,
		prior_year_over_year_date_day,
		day_of_week,
		day_of_week_name,
		day_of_week_name_short,
		day_of_month,
		day_of_year,
		week_start_date,
		prior_year_week_start_date,
		prior_year_week_end_date,
		week_of_year,
		iso_week_start_date,
		iso_week_end_date,
		prior_year_iso_week_start_date,
		prior_year_iso_week_end_date,
		iso_week_of_year,
		prior_year_week_of_year,
		prior_year_iso_week_of_year,
		month_of_year,
		month_name,
		month_name_short,
		month_start_date,
		month_end_date,
		prior_year_month_start_date,
		prior_year_month_end_date,
		quarter_of_year,
		quarter_start_date,
		quarter_end_date,
		year_number,
		year_start_date,
		year_end_date,
		fiscal_week_of_year,
		fiscal_week_of_period,
		fiscal_period_number,
		fiscal_quarter_number,
		fiscal_period_of_quarter,
		case
			when month_day = '01-01' then 'Año Nuevo'
			when date_day = easter_date - interval '48 day' then 'Primer día de Carnaval'
			when date_day = easter_date - interval '47 day' then 'Segundo día de Carnaval'
			when month_day = '03-24' and date_day = easter_date - interval '3 day' then 'Memoria por la Verdad y la Justicia / Jueves Santo'
			when month_day = '03-24' and date_day = easter_date - interval '2 day' then 'Memoria por la Verdad y la Justicia / Viernes Santo'
			when month_day = '03-24' then 'Memoria por la Verdad y la Justicia'
			when month_day = '04-02' and date_day = easter_date - interval '3 day' then 'Día del Veterano / Jueves Santo'
			when month_day = '04-02' and date_day = easter_date - interval '2 day' then 'Día del Veterano / Viernes Santo'
			when month_day = '04-02' then 'Día del Veterano'
			when date_day = easter_date - interval '3 day' then 'Jueves Santo'
			when date_day = easter_date - interval '2 day' then 'Viernes Santo'
			when month_day = '05-01' then 'Día del Trabajador'
			when month_day = '05-25' then 'Día de la Revolución de Mayo'
			when month_day = '06-17' then 'Inmortalidad Gral. Guemes'
			when month_day = '06-20' then 'Inmortalidad Gral. Belgrano'
			when month_day = '07-09' then 'Día de la Independencia'
			when month_day = '08-17' then 'Inmortalidad Gral. San Martín'
			when month_day = '10-12' then 'Día de la raza'
			when month_day = '11-23' then 'Día de la Soberanía Nacional'
			when month_day = '12-08' then 'Inmaculada Concepción de María'
			when month_day = '12-25' then 'Navidad'
			else null
		end as holiday_name

	from step7
)

select * from full_holidays
