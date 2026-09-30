SELECT sale_date
FROM store_sales
WHERE units < 31
UNION
SELECT start_date
FROM subscriptions;
