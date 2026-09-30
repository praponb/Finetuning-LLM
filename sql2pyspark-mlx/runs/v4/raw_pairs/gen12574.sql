select id, delivered_date, order_id from shipments where delivered_date = (select min(delivered_date) from shipments) order by id desc;
