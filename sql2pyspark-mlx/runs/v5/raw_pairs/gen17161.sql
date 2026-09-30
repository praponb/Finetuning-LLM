select month(order_date) as order_month, round(avg(amount), 2) as avg_amount
from orders
where customer_id is null
group by order_month
having avg_amount > 23;
