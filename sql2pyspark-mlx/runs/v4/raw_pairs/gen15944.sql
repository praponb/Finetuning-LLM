WITH filtered_shipments AS (
    SELECT *
    FROM shipments
    WHERE carrier IS NOT NULL AND cost BETWEEN 28 AND 67
),
summary AS (
    SELECT status, MAX(delivered_date) last_delivered_date
    FROM filtered_shipments
    GROUP BY status
)
SELECT status, last_delivered_date
FROM summary
ORDER BY status, last_delivered_date
LIMIT 10
