WITH recent_shipments AS (
    SELECT *, DATEDIFF(shipped_date, delivered_date) days_delivered_date_to_shipped_date
    FROM shipments
    WHERE shipped_date < '2023-06-01'
),
summary AS (
    SELECT DATE_TRUNC('MONTH', shipped_date) shipped_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(carrier))) carrier_values, COUNT(CASE WHEN status = 'LOST' THEN 1 END) lost_count
    FROM recent_shipments
    GROUP BY DATE_TRUNC('MONTH', shipped_date)
)
SELECT *
FROM summary
ORDER BY carrier_values DESC
