select o.id, o.status, o.amount from orders o where not exists (select 1 from shipments s where s.order_id = o.id);
