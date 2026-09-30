SELECT end_date
FROM subscriptions
UNION
SELECT sale_date
FROM store_sales
WHERE store_id = 1
ORDER BY end_date
