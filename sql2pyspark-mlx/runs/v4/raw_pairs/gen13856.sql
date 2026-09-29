WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        attr_key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS attr_key, v
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY created_at DESC, id ASC, attr_key ASC) AS row_num
        FROM exploded
    ) ranked
    WHERE row_num = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN created_at - LAG(created_at) OVER (PARTITION BY attr_key ORDER BY created_at ASC, id ASC, attr_key ASC) > INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM latest
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY attr_key ORDER BY created_at ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(session_start_flag) OVER (PARTITION BY attr_key ORDER BY created_at ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_session_start_flag,
        COUNT(session_start_flag) OVER w_part AS session_start_flag_count_15m
    FROM islands
    WINDOW w_part AS (ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    CASE WHEN session_start_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_start_flag_tier,
    PERCENTILE_APPROX(cumulative_session_start_flag, ARRAY(0.25, 0.5, 0.75)) AS cumulative_session_start_flag_quartiles,
    MAX(session_start_flag) FILTER (WHERE attr_key = 'weight_kg') AS weight_kg_max_session_start_flag,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS event_count,
    GROUPING_ID() AS grouping_level
FROM with_windows
GROUP BY ROLLUP (CASE WHEN session_start_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING MAX(session_start_flag) FILTER (WHERE attr_key = 'weight_kg') < 0
ORDER BY event_count DESC
