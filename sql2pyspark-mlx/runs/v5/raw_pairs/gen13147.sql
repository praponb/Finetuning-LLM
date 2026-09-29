select o.id, o.status, o.discount from orders o where o.discount > (select avg(o2.discount) from orders o2 where o2.status = o.status)
