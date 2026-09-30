WITH filtered_shipments AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), shipped_date) AS days_since_shipped_date
    FROM shipments
    WHERE shipped_date > '2025-03-04'
),
DeliveredMonthSummary AS (
    SELECT DATE_TRUNC('MONTH', delivered_date) AS delivered_month, COUNT(CASE WHEN status = 'LOST' THEN 1 END) AS lost_count
    FROM filtered_shipments
    GROUP BY DATE_TRUNC('MONTH', delivered_date)
)
SELECT delivered_month, lost_count
FROM DeliveredMonthSummary
ORDER BY delivered_month ASC
