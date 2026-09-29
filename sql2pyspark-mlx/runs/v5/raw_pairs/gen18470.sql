SELECT id, units, product_id, sale_date, RANK() OVER (ORDER BY units DESC) AS units_rank, RANK() OVER (PARTITION BY product_id ORDER BY sale_date DESC) AS rnk FROM store_sales WHERE units = 34
