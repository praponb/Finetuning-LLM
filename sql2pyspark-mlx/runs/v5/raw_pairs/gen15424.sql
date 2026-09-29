WITH kept_keys AS (
    -- Set intersection on keys
    SELECT id, store_id
    FROM inventory
    INTERSECT ALL
    SELECT id, store_id
    FROM inventory
    WHERE last_restock >= '2024-05-13'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM inventory s
    JOIN kept_keys k ON s.id = k.id AND s.store_id = k.store_id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', last_restock) AS activity_month,
    MAX(quantity) AS max_quantity,
    COUNT(*) AS cnt,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    COUNT(*) AS row_count,
    COUNT(*) AS event_count
FROM kept_rows
GROUP BY activity_month
HAVING cnt > 1
