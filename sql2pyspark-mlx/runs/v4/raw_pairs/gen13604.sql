select s.id, s.delivered_date from shipments s where month(s.shipped_date) in (3, 4, 10) and s.order_id not in (select id from orders where channel in ('mobile', 'store'));
