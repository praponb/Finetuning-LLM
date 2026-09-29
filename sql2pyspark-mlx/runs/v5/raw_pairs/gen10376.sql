select
    id,
    city,
    rating,
    row_number() over (partition by city order by rating desc, id) as rn
from suppliers;
