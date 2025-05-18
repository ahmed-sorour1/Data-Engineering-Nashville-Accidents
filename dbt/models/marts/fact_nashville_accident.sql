with fact_nashville_accident as(

select 
accident_sk,
accident_id,
date_time,
year,
month,
day,
time,
hour,
minutes,
am_pm,
number_of_motor_vehicles,
number_of_injuries,
number_of_fatalities,
property_damage,
hit_and_run,
collision_type_description,
weather_description,
illumination_description,
harmful_codes,
harmful_descriptions,
object_id,
rpa,
reporting_officer,
street_address,
city,
state,
precinct,
latitude,
longitude,
zip_code,
x,
y

from 
{{ref('int_SK_full_table')}}


),

joins as(

select
accident_sk,
accident_id,
date.date_key,
number_of_motor_vehicles,
number_of_injuries,
number_of_fatalities,
property_damage,
hit_and_run,
location.location_sk,
weather.weather_sk,
collision.collision_sk,
harm.harmful_sk,
rpa,
reporting_officer
from fact_nashville_accident fact
left join {{ref('int_dim_date')}} date on date.date_time = fact.date_time
left join {{ref('int_dim_location')}} location on location.street_address = fact.street_address and location.city = fact.city
                                                                                           and location.state = fact.state    
                                                                                           and location.precinct = fact.precinct
                                                                                           and location.latitude = fact.latitude
                                                                                           and location.longitude = fact.longitude
                                                                                           and location.zip_code = fact.zip_code
                                                                                           and location.x = fact.x    
                                                                                           and location.y = fact.y

left join {{ref('int_dim_weather')}} weather on weather.weather_description = fact.weather_description and weather.illumination_description = fact.illumination_description
left join {{ref('int_dim_collision')}} collision on collision.collision_type_description = fact.collision_type_description
left join {{ref('int_dim_harm')}} harm on harm.harmful_descriptions = fact.harmful_descriptions and harm.harmful_codes = fact.harmful_codes


),

final as(

select * from joins

)

{% if is_incremental() %}
select * from final
where accident_sk not in (select distinct accident_sk from {{ this }})

{% else %}

select * from final

{% endif %}