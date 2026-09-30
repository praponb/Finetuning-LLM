WITH valid AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        INITCAP(TRIM(status)) AS status_title,
        CONCAT_WS('-', status, method) AS status_method_key
    FROM payments
    WHERE method IS DISTINCT FROM 'cash'
      AND status IN ('SUCCESS', 'REFUNDED', 'FAILED')
)
SELECT
    d.status AS order_status,
    s.method,
    COUNT(*) AS rows_total,
    ROUND(AVG(s.amount), 2) AS avg_amount
FROM valid s
LEFT JOIN orders d ON s.order_id = d.id
GROUP BY GROUPING SETS ((d.status, s.method), (d.status), ())
