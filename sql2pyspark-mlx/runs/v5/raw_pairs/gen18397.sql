WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        level,
        CAST(is_remote AS INT) AS is_remote_flag,
        COALESCE(level, 'N/A') AS level_filled
    FROM employees
    WHERE level IS NOT NULL
      AND level IS DISTINCT FROM 'L3'
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        s.id,
        s.full_name,
        s.email,
        s.dept_id,
        s.job_title,
        s.is_remote,
        s.level,
        s.is_remote_flag,
        s.level_filled,
        p.location,
        p.dept_name
    FROM cleaned s
    LEFT JOIN departments p
        ON s.dept_id = p.id
    WHERE p.location <> 'Bangkok'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        job_title,
        is_remote,
        is_remote_flag,
        location,
        MIN(is_remote_flag) OVER w_ordered AS last7_min_is_remote_flag,
        AVG(is_remote_flag) OVER w_ordered AS last7_avg_is_remote_flag,
        COUNT(is_remote_flag) OVER w_ordered AS last7_count_is_remote_flag,
        NTILE(4) OVER (PARTITION BY location ORDER BY is_remote_flag, id) AS is_remote_flag_bucket
    FROM with_dims
    WINDOW w_ordered AS (PARTITION BY dept_id, location ORDER BY is_remote_flag, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(last7_count_is_remote_flag, 0) AS last7_count_is_remote_flag_rounded,
        last7_count_is_remote_flag - is_remote_flag AS last7_count_is_remote_flag_minus_is_remote_flag
    FROM enriched
    WHERE job_title IS NOT NULL
      AND last7_min_is_remote_flag IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY location ORDER BY is_remote_flag DESC, id) AS is_remote_flag_bucket_2,
        AVG(last7_count_is_remote_flag_rounded) OVER (ORDER BY last7_count_is_remote_flag, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last7_count_is_remote_flag_rounded_avg_2,
        SUM(is_remote_flag_bucket) OVER (PARTITION BY location, job_title) AS location_sum_is_remote_flag_bucket
    FROM base
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN is_remote_flag_bucket_2 >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_remote_flag_bucket_2_tier,
    APPROX_PERCENTILE(last7_avg_is_remote_flag, ARRAY(0.25, 0.5, 0.75)) AS last7_avg_is_remote_flag_quartiles,
    COUNT_IF(location = 'Singapore') AS singapore_events,
    COUNT(CASE WHEN location = 'Tokyo' THEN 1 END) AS tokyo_count,
    ROUND(SUM(last7_count_is_remote_flag_rounded_avg_2) / NULLIF(COUNT(CASE WHEN location = 'Tokyo' THEN 1 END), 0), 2) AS last7_count_is_remote_flag_rounded_avg_2_per_tokyo,
    SUM(CASE WHEN location = 'Chiang Mai' THEN last7_count_is_remote_flag_rounded_avg_2 ELSE 0 END) AS chiang_mai_last7_count_is_remote_flag_rounded_avg_2
FROM with_windows
GROUP BY 1
ORDER BY is_remote_flag_bucket_2_tier NULLS FIRST
