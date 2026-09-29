SELECT sale_date AS event_date
FROM store_sales
EXCEPT
SELECT end_date AS event_date
FROM subscriptions;
