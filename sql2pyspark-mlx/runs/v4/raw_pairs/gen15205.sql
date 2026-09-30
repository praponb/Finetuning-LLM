select
    s.*,
    x.payment_paid_at,
    x.payment_method
from orders s
left join lateral (
        select c.paid_at as payment_paid_at, c.method as payment_method
        from payments c
        where c.order_id = s.id
        order by c.paid_at desc, c.id desc
        limit 1
    ) x on true;
