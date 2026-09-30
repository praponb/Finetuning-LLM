select id, order_id, paid_at
from (
    select *, rank() over (partition by order_id order by paid_at) as pos
    from payments
) as ranked
where pos <= 5
