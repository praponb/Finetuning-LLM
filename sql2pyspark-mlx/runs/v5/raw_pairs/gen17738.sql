WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(carrier) AS carrier_length,
        DATEDIFF(delivered_date, shipped_date) AS days_shipped_date_to_delivered_date
    FROM shipments
    WHERE carrier IS DISTINCT FROM 'Flash'
)
-- Aggregate with conditional metrics
SELECT
    order_id,
    DATE_TRUNC('YEAR', shipped_date) AS year_start,
    carrier,
    COUNT(*) AS cnt,
    COUNT(*) AS event_count
FROM valid
GROUP BY order_id, DATE_TRUNC('YEAR', shipped_date), carrier
ORDER BY year_start DESC;
