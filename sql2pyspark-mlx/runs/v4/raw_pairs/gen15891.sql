WITH subscriptions_by_customer AS (
    SELECT customer_id, SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_count, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee
    FROM subscriptions
    GROUP BY customer_id
)
SELECT c.segment, sbc.active_count, sbc.avg_monthly_fee
FROM customers c
LEFT JOIN subscriptions_by_customer sbc ON c.id = sbc.customer_id
