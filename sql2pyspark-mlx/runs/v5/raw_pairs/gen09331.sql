select
    id,
    country,
    city,
    rating,
    count(*) over (partition by country) as country_count,
    row_number() over (partition by city order by rating, id) as rn
from suppliers
where country = 'JP';
