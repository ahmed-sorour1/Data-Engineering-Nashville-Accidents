with int_dim_location as(
select
    street_address,
    city,
    state,
    precinct,
    latitude,
    longitude,
    zip_code,
    x,
    y
  from {{ ref('stg_raw_data__nashville_accident') }}
),

dedup as (
  select distinct * from int_dim_location
),

final as(

select 

{{ dbt_utils.generate_surrogate_key(['street_address', 'city', 'state', 'precinct', 'latitude', 'longitude', 'zip_code', 'x', 'y']) }} AS location_sk,
street_address,
city,
state,
precinct,
latitude,
longitude,
zip_code,
x,
y

from dedup

)

select * from final