with int_SK_full_table as
(

select *
from {{ref('stg_raw_data__nashville_accident')}}


),

final as(

select 

{{ dbt_utils.generate_surrogate_key(['accident_id']) }} as accident_sk,
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
street_address,
city,
state,
precinct,
latitude,
longitude,
harmful_codes,
harmful_descriptions,
object_id,
zip_code,
rpa,
reporting_officer,
x,
y

from int_SK_full_table

)

select * from final