select
    id,
    city,
    region,
    sqft,
    row_number() over (partition by city, region order by sqft desc, id) as sqft_row
from stores
order by sqft_row desc, region;
