with date_spine as (
    select explode(sequence(
        to_date('2016-01-01'),
        to_date('2018-12-31'),
        interval 1 day
    )) as date
),

final as (
    select
        cast(date_format(date, 'yyyyMMdd') as int)  as date_key,
        date                                         as full_date,
        year(date)                                   as year,
        quarter(date)                                as quarter,
        month(date)                                  as month,
        date_format(date, 'MMMM')                   as month_name,
        date_format(date, 'MMM')                    as month_short,
        weekofyear(date)                             as week_of_year,
        dayofmonth(date)                             as day_of_month,
        dayofweek(date)                              as day_of_week,
        date_format(date, 'EEEE')                   as day_name,
        case when dayofweek(date) in (1, 7)
            then true else false end                 as is_weekend,
        date_format(date, 'yyyy-MM')                as year_month,
        concat('Q', quarter(date), ' ', year(date)) as year_quarter
    from date_spine
)

select * from final