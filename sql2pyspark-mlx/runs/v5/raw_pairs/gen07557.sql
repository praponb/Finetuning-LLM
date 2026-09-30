select s.id, s.cost, s.status from shipments s where s.order_id not in (select id from orders where status = 'PENDING') order by status, s.id
