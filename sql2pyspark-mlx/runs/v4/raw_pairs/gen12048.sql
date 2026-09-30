select
    id,
    city,
    region,
    opened_date,
    sqft,
    lead(sqft, 2) over (partition by city, region order by opened_date, id) as next_sqft,
    percent_rank() over (partition by region order by sqft desc) as sqft_pct_rank
from stores
where opened_date between '2023-12-29' and '2025-09-01'
