with metrics as (
    -- Window metrics over several different partitions
    select
        id,
        store_id,
        order_date,
        channel,
        shipping_fee,
        lag(discount, 1) over (order by discount, id) as prev_discount,
        ntile(3) over (partition by customer_id order by shipping_fee, id) as shipping_fee_bucket,
        ntile(10) over (partition by status order by shipping_fee desc, id) as shipping_fee_bucket_2
    from orders
)
select
    id,
    order_date,
    shipping_fee,
    prev_discount,
    shipping_fee_bucket
from metrics
order by shipping_fee_bucket desc nulls last, id;
