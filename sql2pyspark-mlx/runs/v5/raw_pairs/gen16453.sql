select
    o.id,
    o.amount,
    c.segment,
    c.credit_limit,
    s.city,
    s.opened_date,
    greatest(o.amount, o.shipping_fee) as greatest_amount_shipping_fee
from orders o
left join customers c on o.customer_id = c.id
join stores s on o.store_id = s.id
