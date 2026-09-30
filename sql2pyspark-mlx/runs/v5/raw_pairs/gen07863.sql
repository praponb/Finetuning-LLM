select s.id, s.cost, s.delivered_date, s.order_id from shipments s where s.order_id in (22, 30, 38) and s.order_id in (select id from orders where amount < 700) order by s.delivered_date
