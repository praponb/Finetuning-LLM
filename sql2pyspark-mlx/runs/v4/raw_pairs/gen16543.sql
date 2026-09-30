select
    id,
    city,
    rating,
    country,
    min(rating) over (partition by city) as city_min_rating,
    dense_rank() over (partition by country order by rating desc) as rating_dense_rank
from suppliers
where rating > 2.8;
