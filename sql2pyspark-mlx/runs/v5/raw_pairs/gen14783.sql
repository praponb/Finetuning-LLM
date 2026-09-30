select
    id,
    customer_id,
    store_id,
    status,
    channel,
    amount,
    sum(discount) over (partition by channel order by shipping_fee, id rows between unbounded preceding and current row) as running_discount,
    ntile(5) over (partition by customer_id, store_id order by shipping_fee desc, id) as shipping_fee_bucket
from orders
