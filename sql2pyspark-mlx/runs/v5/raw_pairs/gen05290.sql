WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS(',', job_title, level) AS job_title_level_key,
        CASE WHEN level = 'L3' THEN 1 ELSE 0 END AS is_l3
    FROM employees
    WHERE hire_date > DATE '2023-08-16'
      AND job_title IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        job_title,
        level,
        job_title_level_key,
        NTILE(4) OVER (PARTITION BY dept_id ORDER BY is_l3 DESC, id) AS is_l3_bucket,
        FIRST_VALUE(job_title) IGNORE NULLS OVER w_ordered AS first_known_job_title
    FROM cleaned
    WHERE manager_id IN (25, 26, 29)
    WINDOW w_ordered AS (ORDER BY is_l3, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY is_l3_bucket, id) AS rank_in_group
        FROM enriched
    ) ranked
    WHERE rank_in_group = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id, manager_id ORDER BY is_l3_bucket DESC NULLS LAST, id) = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER w_ordered AS is_l3_bucket_bucket,
        PERCENT_RANK() OVER (PARTITION BY level ORDER BY is_l3_bucket) AS is_l3_bucket_percent_rank
    FROM deduped
    WINDOW w_ordered AS (ORDER BY is_l3_bucket, id)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY job_title ORDER BY is_l3_bucket DESC) AS is_l3_bucket_dense_rank,
        LAST_VALUE(job_title) IGNORE NULLS OVER (ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_job_title,
        SUM(is_l3_bucket_percent_rank) OVER w_hist AS manager_id_sum_is_l3_bucket_percent_rank,
        SUM(is_l3_bucket_percent_rank) OVER w_hist_2 AS cumulative_is_l3_bucket_percent_rank,
        NTILE(10) OVER w_ordered AS is_l3_bucket_bucket_2
    FROM metrics
    WINDOW w_hist AS (PARTITION BY manager_id), w_hist_2 AS (PARTITION BY manager_id, dept_id ORDER BY is_l3_bucket_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY dept_id ORDER BY is_l3_bucket, id)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        email,
        manager_id,
        level,
        job_title_level_key,
        is_l3_bucket,
        first_known_job_title,
        is_l3_bucket_bucket,
        is_l3_bucket_percent_rank,
        manager_id_sum_is_l3_bucket_percent_rank,
        cumulative_is_l3_bucket_percent_rank,
        CASE WHEN job_title = 'Analyst' THEN 1 ELSE 0 END AS is_analyst,
        LOWER(email) AS email_lower
    FROM with_windows
    WHERE level IS NOT NULL
      AND is_l3_bucket_bucket <= 5
)
-- Join a dimension and aggregate
SELECT
    p.level AS employee_level,
    p.job_title,
    COUNT(*) AS num_rows,
    MIN(e.is_l3_bucket) AS min_is_l3_bucket,
    ROUND(AVG(e.is_l3_bucket_bucket), 2) AS avg_is_l3_bucket_bucket
FROM valid e
INNER JOIN employees p ON e.manager_id = p.id
GROUP BY GROUPING SETS ((p.level, p.job_title), (p.level), ());
