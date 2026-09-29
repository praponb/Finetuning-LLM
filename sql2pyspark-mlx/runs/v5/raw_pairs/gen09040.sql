select
    id,
    customer_id,
    amount,
    channel
from (
    select *, row_number() over (partition by customer_id order by amount, id) as rnk
    from orders
) as r
where rnk <= 3
order by customer_id;
