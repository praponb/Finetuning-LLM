WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        full_name,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        FIRST_VALUE(level, TRUE) OVER (PARTITION BY dept_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level,
        ROUND(salary / SUM(salary) OVER (PARTITION BY dept_id), 4) AS salary_share
    FROM employees
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN level IN ('L2', 'L5') THEN 'Group A' ELSE 'Other' END AS level_group,
        COALESCE(level, 'OTHER') AS resolved_level
    FROM scored
    WHERE hire_date >= DATE '2023-07-29'
      AND level IS DISTINCT FROM 'L1'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary_share DESC, id) AS rank_in_group
        FROM filtered
    ) ranked
    WHERE rank_in_group = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT level, job_title
    FROM latest
    EXCEPT
    SELECT level, job_title
    FROM employees
    WHERE dept_id = 24
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    JOIN kept_keys k ON s.level = k.level AND s.job_title = k.job_title
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        full_name,
        job_title,
        hire_date,
        is_remote,
        level,
        salary_share,
        resolved_level,
        rank_in_group,
        PERCENT_RANK() OVER (PARTITION BY manager_id ORDER BY salary_share DESC) AS salary_share_percent_rank,
        DENSE_RANK() OVER (ORDER BY salary) AS salary_dense_rank,
        AVG(salary) OVER (PARTITION BY level) AS level_avg_salary,
        CUME_DIST() OVER (PARTITION BY job_title ORDER BY salary DESC) AS salary_cume_dist,
        COUNT(salary) OVER (PARTITION BY job_title) AS job_title_count_salary
    FROM kept_rows
    WHERE MONTH(hire_date) IN (1, 10, 11)
)
-- Grouped metrics
SELECT
    CASE WHEN salary_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS salary_dense_rank_tier,
    DATE_TRUNC('MONTH', hire_date) AS activity_month,
    AVG(job_title_count_salary) FILTER (WHERE level = 'L1') AS l1_avg_job_title_count_salary,
    PERCENTILE_APPROX(level_avg_salary, 0.95) AS level_avg_salary_p95,
    PERCENTILE_APPROX(salary_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS salary_dense_rank_quartiles,
    ROUND(SUM(salary_dense_rank) / NULLIF(COUNT(CASE WHEN job_title = 'Intern' THEN 1 END), 0), 2) AS salary_dense_rank_per_intern
FROM enriched
GROUP BY 1, 2
ORDER BY l1_avg_job_title_count_salary DESC;
