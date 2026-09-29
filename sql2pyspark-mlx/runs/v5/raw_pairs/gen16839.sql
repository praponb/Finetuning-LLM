SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC) AS rnk FROM order_items WHERE product_id IS NULL) AS r WHERE rnk <= 3
