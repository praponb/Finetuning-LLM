WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        status,
        CONCAT_WS('/', status, carrier) AS status_carrier_key
    FROM shipments
    WHERE delivered_date >= DATE '2023-05-16'
      AND cost IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date DESC NULLS FIRST, id) AS latest_rank
        FROM valid
    ) x
    WHERE latest_rank = 1
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT order_id, status
    FROM ranked
    INTERSECT
    SELECT order_id, status
    FROM shipments
    WHERE cost BETWEEN 54 AND 55
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    JOIN remaining_keys k ON s.order_id = k.order_id AND s.status = k.status
)
-- Multi-level aggregation
SELECT
    status,
    SIZE(COLLECT_SET(status)) AS num_status,
    SIZE(COLLECT_SET(status)) AS num_status_2,
    COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END) AS delivered_count
FROM verified
GROUP BY status
HAVING SIZE(COLLECT_SET(status)) < 1
ORDER BY num_status;
