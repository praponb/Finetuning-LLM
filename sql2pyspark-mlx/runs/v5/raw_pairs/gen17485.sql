select s.id, s.delivered_date, s.status, o.discount from shipments s left join orders o on s.order_id = o.id where s.cost is null;
