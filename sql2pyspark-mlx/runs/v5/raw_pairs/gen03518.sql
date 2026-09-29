select
    id,
    country,
    city,
    rating,
    row_number() over (partition by country, city order by rating, id) rating_row
from suppliers
order by country
