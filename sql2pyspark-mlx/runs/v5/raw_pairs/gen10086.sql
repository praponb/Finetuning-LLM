select o.id, o.status, o.shipping_fee, o.channel from orders o where o.shipping_fee > (select avg(o2.shipping_fee) from orders o2 where o2.status = o.status)
