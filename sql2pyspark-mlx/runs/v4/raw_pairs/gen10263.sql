SELECT revenue, product_id, ROUND(revenue, 2) revenue_rounded
FROM store_sales
WHERE units IS NULL
