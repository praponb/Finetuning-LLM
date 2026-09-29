WITH attributed AS (
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM orders s
    INNER JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.order_date >= sub.start_date
       AND s.order_date <= DATE_ADD(sub.end_date, 7)
)
SELECT
    CASE WHEN monthly_fee >= 189 THEN 'LARGE' ELSE 'SMALL' END AS monthly_fee_tier,
    status,
    SIZE(COLLECT_SET(channel)) AS num_channels,
    ROUND(SUM(discount) / NULLIF(COUNT(CASE WHEN channel = 'mobile' THEN 1 END), 0), 2) AS discount_per_mobile
FROM attributed
GROUP BY CASE WHEN monthly_fee >= 189 THEN 'LARGE' ELSE 'SMALL' END, status
HAVING num_channels >= 1
ORDER BY discount_per_mobile DESC
