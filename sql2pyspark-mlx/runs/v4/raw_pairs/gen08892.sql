SELECT customer_id, total_monthly_fee
FROM (
    SELECT customer_id, SUM(monthly_fee) AS total_monthly_fee
    FROM subscriptions
    GROUP BY customer_id
) AS t
WHERE total_monthly_fee >= 1390
ORDER BY total_monthly_fee DESC
