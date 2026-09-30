select
    id,
    category,
    launch_date,
    price,
    round(avg(price) over (partition by category order by launch_date, id rows between 3 preceding and current row), 2) as price_moving_avg_4
from products
where brand = 'Umbrella';
