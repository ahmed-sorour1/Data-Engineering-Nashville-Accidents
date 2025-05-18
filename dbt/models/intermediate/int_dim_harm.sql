with int_dim_harm as(

select distinct
harmful_descriptions,
harmful_codes
from {{ref('stg_raw_data__nashville_accident')}}
),

final as(

select 

{{ dbt_utils.generate_surrogate_key(['harmful_descriptions', 'harmful_codes']) }} AS harmful_sk,
harmful_descriptions,
harmful_codes
from int_dim_harm

)

select * from final