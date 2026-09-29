select
    id,
    country,
    rating,
    city,
    round(rating / sum(rating) over (partition by country), 3) as rating_share,
    sum(rating) over (partition by city, country order by rating, id rows between unbounded preceding and current row) as running_rating
from suppliers
