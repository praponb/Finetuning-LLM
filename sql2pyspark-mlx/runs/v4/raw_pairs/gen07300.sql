select
    id,
    launch_date,
    price,
    lead(price, 1, 0) over (order by launch_date, id) as next_price
from products
where month(launch_date) in (3, 11, 12)
order by price desc
