WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), started_at) AS days_since_started_at,
        COALESCE(country, 'UNKNOWN') AS resolved_country
    FROM sessions
    WHERE started_at > TO_DATE('2023-09-06')
      AND customer_id IS NOT NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM cleaned
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY device ORDER BY days_since_started_at DESC NULLS FIRST) AS row_num
        FROM islands
    ) t
    WHERE row_num = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(is_new_burst) OVER (PARTITION BY browser ORDER BY is_new_burst ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_is_new_burst,
        MIN(is_new_burst) OVER (PARTITION BY browser ORDER BY is_new_burst ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_is_new_burst,
        MAX(is_new_burst) OVER (PARTITION BY browser ORDER BY is_new_burst ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_is_new_burst,
        COUNT(is_new_burst) OVER (PARTITION BY browser ORDER BY is_new_burst ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_is_new_burst
    FROM ranked
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY is_new_burst DESC, id ASC) AS dedup_rank
        FROM with_windows
    ) t
    WHERE dedup_rank = 1
)
-- Running and moving aggregates
SELECT
    *,
    SUM(burst_id) OVER (ORDER BY dedup_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_burst_id,
    NTILE(4) OVER (PARTITION BY country ORDER BY days_since_started_at DESC, id ASC) AS days_since_started_at_bucket
FROM latest
WHERE is_new_burst >= 1
