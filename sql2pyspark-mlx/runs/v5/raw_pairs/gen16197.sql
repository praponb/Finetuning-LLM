WITH dim_totals AS (
    SELECT
        p.city,
        COUNT(*) AS rows_total,
        AVG(a.quantity) AS avg_quantity
    FROM inventory a
    LEFT JOIN stores p ON a.store_id = p.id
    GROUP BY CUBE (p.city)
)
SELECT
    city,
    rows_total,
    avg_quantity
FROM dim_totals
ORDER BY avg_quantity DESC NULLS FIRST;
