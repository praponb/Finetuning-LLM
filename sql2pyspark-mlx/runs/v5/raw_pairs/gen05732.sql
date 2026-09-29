select
    s.order_id,
    s.carrier,
    o.shipping_fee,
    c.credit_limit,
    c.age,
    least(o.shipping_fee, o.discount) as least_shipping_fee_discount
from shipments s
left join orders o on o.id = s.order_id
right join customers c on o.customer_id = c.id
where c.credit_limit <= 8600;
