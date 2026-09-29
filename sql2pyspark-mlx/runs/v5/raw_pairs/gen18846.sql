select
    id,
    customer_id,
    store_id,
    order_date,
    status,
    channel,
    amount,
    discount,
    row_number() over (partition by customer_id order by discount desc, id asc) as event_seq,
    avg(discount) over (partition by store_id order by order_date asc, id asc rows between 5 preceding and 1 preceding) as discount_avg_5_prior
from orders;
