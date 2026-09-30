with remaining_keys as (
    select status, store_id
    from orders
    intersect
    select status, store_id
    from orders
    where shipping_fee >= 18
),
verified as (
    select s.*
    from orders s
    inner join remaining_keys k on s.status = k.status and s.store_id = k.store_id
)
select
    date_trunc('quarter', order_date) as quarter_start,
    customer_id,
    date_trunc('week', order_date) as week_start,
    stddev_samp(shipping_fee) as shipping_fee_stddev,
    stddev_samp(discount) as discount_stddev,
    sum(shipping_fee) as total_shipping_fee,
    dense_rank() over (partition by date_trunc('quarter', order_date) order by stddev_samp(shipping_fee) desc) as shipping_fee_stddev_dense_rank
from verified
group by 1, 2, 3
order by shipping_fee_stddev desc nulls last;
