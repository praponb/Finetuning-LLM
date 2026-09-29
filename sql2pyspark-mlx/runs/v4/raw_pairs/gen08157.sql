SELECT id, store_id, sale_date, product_id FROM (SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY sale_date) AS rnk FROM store_sales) AS r WHERE rnk <= 2 ORDER BY store_id ASC, sale_date ASC;
