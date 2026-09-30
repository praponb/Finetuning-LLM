select
    p.paid_at,
    o.customer_id,
    o.discount,
    c.credit_limit
from payments p
left join orders o on p.order_id = o.id
inner join customers c on c.id = o.customer_id
where p.status in ('FAILED', 'SUCCESS', 'REFUNDED');
