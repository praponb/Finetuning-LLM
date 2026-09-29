select
    id,
    country,
    rating,
    city,
    percent_rank() over (partition by country order by rating desc) as rating_pct_rank,
    last_value(country) over (partition by city order by rating desc, id rows between unbounded preceding and unbounded following) as last_country
from suppliers;
