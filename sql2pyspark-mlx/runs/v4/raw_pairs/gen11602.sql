SELECT status, monthly_fee
FROM subscriptions
WHERE start_date > '2023-09-01' AND YEAR(end_date) >= 2024;
