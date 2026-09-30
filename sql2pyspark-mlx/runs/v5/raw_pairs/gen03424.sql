SELECT *, RANK() OVER (PARTITION BY store_id, product_id ORDER BY sale_date DESC NULLS LAST) AS rn
FROM store_sales
QUALIFY rn = 1
