select
    id,
    city,
    sqft,
    ntile(4) over (partition by city order by sqft, id) bucket
from stores
