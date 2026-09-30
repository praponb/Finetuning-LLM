WITH recent_shipments AS (
    SELECT *, DATE_FORMAT(delivered_date, 'yyyy-MM') AS delivered_date_label
    FROM shipments
    WHERE shipped_date BETWEEN '2023-06-16' AND '2025-07-01' AND delivered_date > '2024-11-20' AND MONTH(delivered_date) IN (5, 8, 12)
),
shipments_stats AS (
    SELECT
        QUARTER(shipped_date) AS shipped_quarter,
        DATE_TRUNC('MONTH', shipped_date) AS shipped_month,
        MIN(cost) AS min_cost,
        MAX(shipped_date) AS latest_shipped_date
    FROM recent_shipments
    GROUP BY QUARTER(shipped_date), DATE_TRUNC('MONTH', shipped_date)
)
SELECT *
FROM shipments_stats
WHERE min_cost < 60
ORDER BY shipped_quarter DESC
