SELECT id, status, monthly_fee
FROM subscriptions
WHERE monthly_fee BETWEEN 170 AND 200 OR MONTH(start_date) IN (2, 3, 10);
