SELECT plan, MIN(monthly_fee) AS min_monthly_fee
FROM subscriptions
WHERE start_date > '2024-08-01' AND end_date BETWEEN '2025-02-01' AND '2025-04-01'
GROUP BY plan
HAVING MIN(monthly_fee) < 160;
