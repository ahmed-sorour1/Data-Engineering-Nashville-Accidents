with stg_raw_data__nashville_accident as (
    select * 
    from {{ source('raw_nashville_accident', 'nashville_accident') }}
),

renamed as (
    select
        "Accident Number"            as accident_id,
        "Date and Time"              as date_time,
        year,
        month,
        day,
        "Time"                       as time,
        hour,
        minutes,
        "PM/AM"                      as am_pm,
        "Number of Motor Vehicles"   as number_of_motor_vehicles,
        "Number of Injuries"         as number_of_injuries,
        "Number of Fatalities"       as number_of_fatalities,
        "Property Damage"            as property_damage,
        "Hit and Run"                as hit_and_run,
        "Collision Type Description" as collision_type_description,
        "Weather Description"        as weather_description,
        "Illumination Description"   as illumination_description,
        "HarmfulCodes"               as harmful_codes,
        "HarmfulDescriptions"        as harmful_descriptions,
        "ObjectId"                   as object_id,
        "RPA"                        as rpa,
        "Reporting Officer"          as reporting_officer,
        trim(upper("Street Address")) as street_address,
        trim(upper("City")) as city,
        trim(upper("State")) as state,
        "Precinct"                   as precinct,
        round("Lat", 4) as latitude,
        round("Long", 4) as longitude,
        "Zip Code"                   as zip_code,
        round(x, 2) as x,
        round(y, 2) as y

    from stg_raw_data__nashville_accident
)

select *
from renamed
