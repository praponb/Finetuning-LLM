WITH base AS (
    -- Keep recent, valid rows
    SELECT *, CAST(is_remote AS INT) AS is_remote_flag
    FROM employees
    WHERE hire_date > DATE '2023-12-20'
      AND manager_id IS NOT NULL
      AND full_name LIKE '%bob%'
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        e.*,
        p2.hire_date AS employee_hire_date,
        p2.full_name AS employee_full_name
    FROM base e
    LEFT JOIN employees p2
        ON e.manager_id = p2.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        is_remote_flag,
        employee_hire_date,
        employee_full_name,
        PERCENT_RANK() OVER (PARTITION BY job_title, level ORDER BY is_remote_flag NULLS LAST) AS is_remote_flag_percent_rank,
        NTILE(4) OVER (PARTITION BY job_title ORDER BY is_remote_flag DESC, id) AS is_remote_flag_bucket,
        FIRST_VALUE(is_remote_flag) IGNORE NULLS OVER (PARTITION BY level ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_is_remote_flag,
        MAX(is_remote_flag) OVER (PARTITION BY job_title) AS job_title_max_is_remote_flag,
        LAG(salary, 1) OVER (PARTITION BY dept_id ORDER BY hire_date, id) AS prev_salary
    FROM with_dims
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id, level ORDER BY salary DESC, id) AS dedup_rank
        FROM metrics
    ) ranked
    WHERE dedup_rank = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(4) OVER w_recent AS salary_bucket,
        CUME_DIST() OVER w_recent_2 AS is_remote_flag_percent_rank_cume_dist
    FROM ranked
    WINDOW w_recent AS (ORDER BY salary, id), w_recent_2 AS (PARTITION BY manager_id ORDER BY is_remote_flag_percent_rank NULLS LAST)
),
enriched AS (
    -- Attach employees attributes
    SELECT a.*, d.salary AS employee_salary
    FROM scored a
    INNER JOIN employees d
        ON a.manager_id = d.id
),
scored_employees AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY is_remote_flag_percent_rank_cume_dist DESC) AS is_remote_flag_percent_rank_cume_dist_rank,
        RANK() OVER (ORDER BY is_remote_flag DESC) AS is_remote_flag_rank
    FROM enriched
)
-- Running and moving aggregates
SELECT
    *,
    FIRST_VALUE(is_remote_flag_rank) IGNORE NULLS OVER (ORDER BY is_remote_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_is_remote_flag_rank,
    ROUND(is_remote_flag_percent_rank_cume_dist_rank / SUM(is_remote_flag_percent_rank_cume_dist_rank) OVER (PARTITION BY dept_id), 4) AS is_remote_flag_percent_rank_cume_dist_rank_share,
    AVG(job_title_max_is_remote_flag) OVER (PARTITION BY manager_id) AS manager_id_avg_job_title_max_is_remote_flag,
    AVG(is_remote_flag_percent_rank_cume_dist_rank) OVER w_hist AS level_avg_is_remote_flag_percent_rank_cume_dist_rank,
    LAG(salary_bucket, 1) OVER w_running AS prev_salary_bucket
FROM scored_employees
WINDOW w_hist AS (PARTITION BY level, dept_id), w_running AS (ORDER BY is_remote_flag, id);
