WITH matched_keys AS (
    SELECT order_id, id
    FROM payments
    EXCEPT DISTINCT
    SELECT order_id, id
    FROM payments
    WHERE method = 'cash'
),
kept_rows AS (
    SELECT s.*
    FROM payments s
    JOIN matched_keys k ON s.order_id = k.order_id AND s.id = k.id
)
SELECT
    CASE WHEN amount >= 1640 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    COUNT(CASE WHEN method = 'paypal' THEN 1 END) AS paypal_count,
    COUNT(*) FILTER (WHERE method = 'card') AS card_rows,
    COUNT(*) FILTER (WHERE status = 'REFUNDED') AS refunded_rows
FROM kept_rows
GROUP BY 1, 2
ORDER BY card_rows NULLS LAST;
