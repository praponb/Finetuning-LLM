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
    percent_rank() over w_part as discount_percent_rank,
    avg(shipping_fee) over (partition by customer_id, store_id order by discount, id rows between 2 preceding and 1 preceding) as shipping_fee_avg_2_prior
from orders
window w_part as (order by discount desc);
