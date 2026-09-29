SELECT start_date
FROM subscriptions
WHERE YEAR(start_date) = 2024
UNION
SELECT opened_date
FROM stores
WHERE city = 'Chiang Mai';
