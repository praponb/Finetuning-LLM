select o.id, o.amount, o.shipping_fee from orders o where o.customer_id in (4, 12, 25, 32) and exists (select 1 from shipments s where s.order_id = o.id and s.cost <= 47)
