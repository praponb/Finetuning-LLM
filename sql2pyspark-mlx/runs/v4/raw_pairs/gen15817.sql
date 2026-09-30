SELECT status
FROM subscriptions
WHERE start_date BETWEEN '2025-02-02' AND '2025-09-07'
UNION ALL
SELECT status
FROM subscriptions
WHERE monthly_fee > 180
