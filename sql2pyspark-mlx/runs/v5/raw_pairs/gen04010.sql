select product_id, avg(units) avg_units, max(revenue) max_revenue from store_sales group by product_id order by avg_units, product_id limit 3;
