select id, order_id, delivered_date, carrier from (select *, row_number() over (partition by order_id order by delivered_date desc, id) as row_num from shipments) as t where row_num <= 5
