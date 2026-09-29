select s.id, s.carrier, s.delivered_date, s.order_id from shipments s where s.order_id not in (select id from orders where status = 'SHIPPED') order by s.order_id desc;
