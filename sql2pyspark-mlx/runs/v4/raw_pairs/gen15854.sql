select
    p.amount,
    p.paid_at,
    o.shipping_fee,
    c.age,
    c.email,
    case when p.method in ('card', 'promptpay') then 'Tier 1' else 'Other' end as method_group
from payments p
left outer join orders o on p.order_id = o.id
inner join customers c on o.customer_id = c.id
where o.store_id is not null
