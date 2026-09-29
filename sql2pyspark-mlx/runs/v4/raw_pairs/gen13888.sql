select id, category, launch_date, price, lag(price, 2) over (partition by category order by launch_date, id) as prev_price from products where cost <= 600;
