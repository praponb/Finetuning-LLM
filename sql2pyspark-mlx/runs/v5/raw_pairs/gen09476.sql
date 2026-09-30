WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        store_name,
        city
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC, id) AS row_num
        FROM stores
    ) ranked
    WHERE row_num = 1
)
-- Aggregate with conditional metrics
SELECT
    city,
    COUNT(*) FILTER (WHERE city = 'Tokyo') AS tokyo_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT_IF(city = 'London') AS london_events
FROM deduped
GROUP BY city
ORDER BY london_events
