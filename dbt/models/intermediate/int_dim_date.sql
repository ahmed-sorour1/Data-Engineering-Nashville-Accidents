with int_dim_date as (

  select distinct
    date_time,
    year,
    month,
    day,
    time,
    hour,
    minutes,
    am_pm

  from {{ ref('stg_raw_data__nashville_accident') }}

),

final as (
select
   cast(
      concat(
        year,
        lpad(cast(month as text), 2, '0'),
        lpad(cast(day as text), 2, '0'),
        lpad(cast(hour as text), 2, '0'),
        lpad(cast(minutes as text), 2, '0')
      ) as int
    ) as date_key,
    date_time,
    year,
    month,
    day,
    time,
    hour,
    minutes,
    am_pm, 
    case 
        when hour between 5 and 11 then 'Morning'
        when hour between 12 and 16 then 'Afternoon'
        when hour between 17 and 20 then 'Evening'
        else 'Night'
           end as day_time


  from int_dim_date

)

select * from final
