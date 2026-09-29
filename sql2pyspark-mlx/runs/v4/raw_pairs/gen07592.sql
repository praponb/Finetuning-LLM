select id, order_id, cost, delivered_date from (select id, order_id, cost, delivered_date, row_number() over (partition by order_id order by cost desc, id) as pos from shipments) as r where pos = 1
