select year(order_date) as order_year, count(amount) as amount_count, min(discount) as min_discount
from orders
where shipping_fee between 25 and 27
group by year(order_date);
