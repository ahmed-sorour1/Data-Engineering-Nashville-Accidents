with int_dim_weather as(

select distinct
weather_description,
illumination_description
from {{ ref('stg_raw_data__nashville_accident') }}
),

final as(

select 

{{ dbt_utils.generate_surrogate_key(['weather_description', 'illumination_description']) }} AS weather_sk,
weather_description,
illumination_description

from int_dim_weather

)

select * from final