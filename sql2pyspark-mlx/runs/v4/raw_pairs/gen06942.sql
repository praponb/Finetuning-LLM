select s.id, s.order_id, o.store_id from shipments s left outer join orders o on o.id = s.order_id where s.status = 'DELIVERED' order by order_id desc, 3 desc
