select
    city,
    region,
    sqft,
    round(sqft, 2) as sqft_rounded
from stores
where region is null
order by sqft desc
