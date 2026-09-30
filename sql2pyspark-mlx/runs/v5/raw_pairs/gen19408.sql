select
    id,
    country,
    rating,
    sum(rating) over (partition by country) as country_sum_rating
from suppliers
where length(supplier_name) >= 7
order by rating desc
