WITH cleaned AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), paid_at) AS days_since_paid_at,
        COALESCE(method, 'DIRECT') AS resolved_method
    FROM payments
    WHERE status = 'SUCCESS'
)
SELECT
    dim.channel,
    e.status AS payment_status,
    COUNT(*) AS num_rows,
    SUM(e.amount) AS total_amount,
    GROUPING_ID() AS grouping_level
FROM cleaned e
INNER JOIN orders dim ON e.order_id = dim.id
GROUP BY ROLLUP (dim.channel, e.status)
ORDER BY channel DESC NULLS LAST
