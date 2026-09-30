SELECT status, MIN(monthly_fee) AS min_monthly_fee
FROM subscriptions
WHERE monthly_fee < 230 AND customer_id IS NULL
GROUP BY status
HAVING MIN(monthly_fee) < 160
ORDER BY status;
