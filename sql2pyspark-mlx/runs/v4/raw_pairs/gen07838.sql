select
    channel,
    year(order_date) as order_year,
    customer_id,
    round(stddev(amount), 2) as stddev_amount
from orders
where customer_id in (5, 19, 24, 36) and amount <= 1390 and amount >= 890
group by channel, year(order_date), customer_id
having round(stddev(amount), 2) < 52;
