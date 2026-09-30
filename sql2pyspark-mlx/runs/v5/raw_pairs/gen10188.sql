SELECT opened_date
FROM stores
WHERE region = 'East'
EXCEPT
SELECT start_date AS opened_date
FROM subscriptions;
