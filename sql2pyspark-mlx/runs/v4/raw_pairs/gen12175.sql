select
    id,
    customer_id,
    store_id,
    order_date,
    status,
    channel,
    amount,
    discount,
    shipping_fee,
    ntile(3) over (partition by customer_id, channel order by amount, id) as amount_bucket,
    last_value(discount, true) over (partition by customer_id, status order by discount, id rows between unbounded preceding and current row) as last_known_discount
from orders
