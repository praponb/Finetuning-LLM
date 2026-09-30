SELECT end_date
FROM subscriptions
WHERE end_date > '2023-04-01'
UNION ALL
SELECT sale_date
FROM store_sales
