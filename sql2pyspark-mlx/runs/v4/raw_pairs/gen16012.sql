SELECT id, units, RANK() OVER (ORDER BY units DESC) AS rnk FROM store_sales WHERE product_id IN (2, 3, 6, 10)
