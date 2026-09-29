WITH cleaned AS (
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        status,
        LOWER(status) AS status_lower,
        COALESCE(status, 'N/A') AS status_clean
    FROM accounts
    WHERE status IS NOT NULL
      AND status IS NOT DISTINCT FROM 'CLOSED'
)
SELECT
    d.city,
    s.status,
    COUNT(*) AS rows_total,
    MAX(s.balance) AS max_balance,
    GROUPING_ID() AS level_id
FROM cleaned s
LEFT JOIN customers d ON s.customer_id = d.id
GROUP BY GROUPING SETS ((d.city, s.status), (d.city), ());
