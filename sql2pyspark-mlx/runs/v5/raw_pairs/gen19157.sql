select product_id, max(units) as max_units from store_sales group by product_id having max(units) > 18 order by max_units desc, product_id desc limit 10
