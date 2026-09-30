select
    id,
    city,
    rating,
    lag(rating, 2) over (partition by city order by rating, id) as prev_rating
from suppliers
where supplier_name not like 'Lio%'
order by 2 asc
