select
    p.id,
    p.amount,
    o.channel,
    o.amount as order_amount,
    c.is_active,
    date_format(p.paid_at, 'yyyy-MM') as paid_at_label
from payments p
left join orders o on o.id = p.order_id
inner join customers c on o.customer_id = c.id
where o.channel = 'partner' or o.shipping_fee >= 12
