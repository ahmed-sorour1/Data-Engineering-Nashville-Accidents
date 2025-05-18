with int_dim_collision as(

select distinct
collision_type_description
from {{ ref('stg_raw_data__nashville_accident') }}
),

final as(

select 

{{ dbt_utils.generate_surrogate_key(['collision_type_description']) }} AS collision_sk,
collision_type_description

from int_dim_collision

)

select * from final