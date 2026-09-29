select o.discount, p.order_id from orders o left outer join payments p on o.id = p.order_id
