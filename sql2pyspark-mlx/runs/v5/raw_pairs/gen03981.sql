select id, units, sale_date, revenue, floor(units) as units_whole from store_sales where units != 19 and store_id in (2, 6) order by sale_date, id limit 5;
