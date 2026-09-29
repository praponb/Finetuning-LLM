SELECT end_date, 'subscription' AS source
FROM subscriptions
UNION ALL
SELECT sale_date, 'store_sale' AS source
FROM store_sales
WHERE product_id IN (1, 2)
