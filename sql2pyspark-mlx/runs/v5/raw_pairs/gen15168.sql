select
    id,
    customer_id,
    store_id,
    channel,
    avg(shipping_fee) over (partition by customer_id order by discount, id rows between unbounded preceding and current row) as running_avg_shipping_fee,
    sum(shipping_fee) over (partition by customer_id order by discount, id rows between unbounded preceding and current row) as running_sum_shipping_fee,
    max(shipping_fee) over (partition by customer_id order by discount, id rows between unbounded preceding and current row) as running_max_shipping_fee,
    count(shipping_fee) over (partition by customer_id order by discount, id rows between unbounded preceding and current row) as running_count_shipping_fee
from orders
where store_id = 13;
