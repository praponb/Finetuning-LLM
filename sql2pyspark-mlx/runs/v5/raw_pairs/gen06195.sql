select product_id, count(units) as units_count, avg(units) as avg_units from store_sales where product_id = 3 group by product_id;
