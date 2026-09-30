WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        k,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS k, value
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(k) AS k_upper,
        COALESCE(k, 'NONE') AS k_filled
    FROM exploded
    WHERE created_at >= TO_DATE('2025-03-02')
      AND customer_id IS NOT NULL
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        created_at,
        k,
        value,
        k_upper,
        k_filled,
        CAST(created_at AS DATE) AS created_at_day,
        CASE WHEN k = 'utm_source' THEN 1 ELSE 0 END AS is_utm_source
    FROM valid
    WHERE k IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY created_at_day DESC, id, k) AS latest_rank
        FROM filtered
    ) t
    WHERE latest_rank = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN created_at - LAG(created_at) OVER (PARTITION BY k ORDER BY created_at, id, k) > INTERVAL '1' HOUR THEN 1 ELSE 0 END AS is_new_burst
    FROM ranked
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        created_at,
        k,
        created_at_day,
        is_utm_source,
        latest_rank,
        SUM(is_new_burst) OVER (PARTITION BY k ORDER BY created_at, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
-- Grouped metrics
SELECT
    k,
    ROUND(AVG(is_utm_source), 2) AS avg_is_utm_source,
    PERCENTILE_APPROX(session_no, 0.9) AS session_no_p90,
    AVG(is_utm_source) AS avg_is_utm_source_2
FROM sessionized
GROUP BY k
ORDER BY k DESC NULLS FIRST
