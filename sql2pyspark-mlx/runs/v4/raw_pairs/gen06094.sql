select
    channel,
    customer_id,
    min(discount) as min_discount,
    sum(amount) as total_amount
from orders
where channel in ('mobile', 'partner', 'store') and shipping_fee between 11 and 23 and month(order_date) in (2, 4, 5)
group by channel, customer_id
order by min_discount, total_amount;
