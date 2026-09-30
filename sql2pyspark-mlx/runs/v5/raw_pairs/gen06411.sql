WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        k,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS k, value
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN created_at - LAG(created_at) OVER (PARTITION BY k ORDER BY created_at, id, k) > INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM flattened
),
sessions_carts AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY k ORDER BY created_at, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY created_at DESC, id, k) AS row_num
    FROM sessions_carts
    QUALIFY row_num = 1
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM deduped
    INTERSECT
    SELECT id
    FROM carts
    WHERE customer_id IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN matched_keys k ON s.id = k.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN is_new_burst >= 1 THEN 'HIGH' ELSE 'LOW' END AS is_new_burst_tier,
    ROUND(SUM(burst_id) / NULLIF(COUNT(CASE WHEN k = 'gift' THEN 1 END), 0), 2) AS burst_id_per_gift,
    AVG(is_new_burst) FILTER (WHERE k = 'gift') AS gift_avg_is_new_burst,
    MAX(burst_id) FILTER (WHERE k = 'device') AS device_max_burst_id,
    COUNT(CASE WHEN k = 'device' THEN 1 END) AS device_count,
    MAX(burst_id) FILTER (WHERE k = 'gift') AS gift_max_burst_id
FROM verified
GROUP BY is_new_burst_tier
ORDER BY gift_avg_is_new_burst DESC;
