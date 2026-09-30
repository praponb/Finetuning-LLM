SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY units NULLS FIRST) AS rn
FROM store_sales
QUALIFY rn = 1
