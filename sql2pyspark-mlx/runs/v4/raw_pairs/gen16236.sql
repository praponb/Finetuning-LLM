select
    *,
    ntile(5) over (order by discount desc, id) discount_bucket,
    lag(amount, 1) over (partition by store_id order by discount, id) prev_amount,
    lag(amount, 1, 0) over (partition by status order by amount, id) prev_amount_2,
    sum(discount) over (partition by customer_id, store_id order by amount, id rows between unbounded preceding and current row) cumulative_discount,
    row_number() over (partition by customer_id order by order_date desc, id) event_seq
from orders
