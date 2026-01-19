with date_spine as (
    -- Generate dates for the last 5 years up to 1 year in the future
    select
        (current_date - interval '5 year' + (n || ' days')::interval)::date as full_date
    from generate_series(0, 365*6) n
),

date_dims as (
    select
        full_date,
        cast(to_char(full_date, 'YYYYMMDD') as integer) as date_key,
        extract(dow from full_date) as day_of_week,
        to_char(full_date, 'Day') as day_name,
        extract(week from full_date) as week_of_year,
        extract(month from full_date) as month,
        to_char(full_date, 'Month') as month_name,
        extract(quarter from full_date) as quarter,
        extract(year from full_date) as year,
        case when extract(dow from full_date) in (0, 6) then true else false end as is_weekend
    from date_spine
)

select * from date_dims
