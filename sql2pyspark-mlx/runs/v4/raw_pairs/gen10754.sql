WITH base AS (
    SELECT *
    FROM shipments
    WHERE shipped_date BETWEEN '2024-01-12' AND '2024-02-01'
),
shipments_stats AS (
    SELECT carrier, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
    FROM base
    GROUP BY carrier
)
SELECT carrier, status_values
FROM shipments_stats
ORDER BY status_values;
