WITH filtered_shipments AS (
    SELECT *, DATEDIFF(shipped_date, delivered_date) AS days_delivered_date_to_shipped_date
    FROM shipments
    WHERE cost <= 74
),
summary AS (
    SELECT status, CASE WHEN days_delivered_date_to_shipped_date >= 82 THEN 'high' WHEN days_delivered_date_to_shipped_date >= 11 THEN 'mid' ELSE 'low' END AS days_delivered_date_to_shipped_date_bucket, MAX(delivered_date) AS last_delivered_date
    FROM filtered_shipments
    GROUP BY status, CASE WHEN days_delivered_date_to_shipped_date >= 82 THEN 'high' WHEN days_delivered_date_to_shipped_date >= 11 THEN 'mid' ELSE 'low' END
)
SELECT status, days_delivered_date_to_shipped_date_bucket, last_delivered_date
FROM summary
ORDER BY 3, status, days_delivered_date_to_shipped_date_bucket
LIMIT 10;
