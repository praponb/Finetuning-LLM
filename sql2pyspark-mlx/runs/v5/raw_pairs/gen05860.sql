SELECT 'subscription' AS source, monthly_fee
FROM subscriptions
WHERE YEAR(end_date) = 2025
UNION
SELECT 'order' AS source, amount
FROM orders
WHERE YEAR(order_date) = 2023;
