select
    id,
    city,
    sqft,
    ntile(4) over (partition by region order by sqft desc, id) as sqft_bucket,
    ntile(4) over (order by sqft desc, id) as sqft_bucket_2
from stores
