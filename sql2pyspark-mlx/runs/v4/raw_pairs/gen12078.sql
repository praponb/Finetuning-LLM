select
    s.*,
    x.paid_at,
    x.payment_amount
from orders s
join lateral (
        select c.paid_at, c.amount as payment_amount
        from payments c
        where c.order_id = s.id
        order by c.amount desc, c.id desc
        limit 1
    ) x on true
