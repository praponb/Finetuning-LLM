select id, order_id, shipped_date, row_number() over (partition by order_id order by shipped_date, id) as row_num from shipments order by 3;
