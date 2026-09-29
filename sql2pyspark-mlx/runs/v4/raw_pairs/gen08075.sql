WITH with_latest AS (
    SELECT
        s.*,
        latest.paid_at,
        latest.payment_amount
    FROM orders s
    JOIN LATERAL (
            SELECT c.paid_at, c.amount AS payment_amount
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at <= s.order_date
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    CASE WHEN amount >= 1660 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    DATE_TRUNC('MONTH', order_date) AS activity_month,
    MAX(paid_at) AS last_seen,
    SUM(amount) FILTER (WHERE status = 'SHIPPED') AS shipped_total_amount,
    SUM(CASE WHEN channel = 'store' THEN discount ELSE 0 END) AS store_discount,
    MIN(paid_at) AS first_seen,
    SIZE(COLLECT_SET(status)) AS num_status
FROM with_latest
GROUP BY CASE WHEN amount >= 1660 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('MONTH', order_date)
HAVING SUM(amount) FILTER (WHERE status = 'SHIPPED') > 0
ORDER BY activity_month NULLS LAST;
