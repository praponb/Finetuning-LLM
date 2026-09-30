select
    id,
    country,
    city,
    rating,
    lead(rating, 1) over (partition by country, city order by rating, id) as next_rating,
    sum(rating) over (partition by country) as country_sum_rating
from suppliers
order by next_rating desc
