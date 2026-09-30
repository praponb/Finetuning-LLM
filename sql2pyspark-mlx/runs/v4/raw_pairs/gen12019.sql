select month(order_date) as order_month, count(shipping_fee) as shipping_fee_count, max(discount) as max_discount
from orders
where customer_id = 34 or discount >= 0.31
group by order_month
order by shipping_fee_count desc
