WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(is_remote AS INT) AS is_remote_flag,
        LAST_DAY(hire_date) AS hire_date_month_end,
        COALESCE(level, 'OTHER') AS resolved_level
    FROM employees
    WHERE level IS NOT NULL
      AND hire_date > DATE '2023-05-09'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title, dept_id ORDER BY salary DESC, id ASC) AS rank_in_group
    FROM base
    QUALIFY rank_in_group = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY job_title ORDER BY is_remote_flag DESC NULLS LAST) AS is_remote_flag_percent_rank,
        CUME_DIST() OVER (PARTITION BY dept_id ORDER BY is_remote_flag DESC) AS is_remote_flag_cume_dist,
        CUME_DIST() OVER w_ordered AS salary_cume_dist,
        PERCENT_RANK() OVER (ORDER BY salary ASC) AS salary_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date_month_end ASC, id ASC) AS seq_no
    FROM ranked
    WINDOW w_ordered AS (PARTITION BY level ORDER BY salary ASC)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER w_part AS seq_no_2,
        SUM(salary_cume_dist) OVER (PARTITION BY job_title ORDER BY seq_no ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary_cume_dist,
        LEAD(salary_percent_rank, 1) OVER (ORDER BY is_remote_flag_percent_rank ASC, id ASC) AS next_salary_percent_rank,
        ROW_NUMBER() OVER (ORDER BY seq_no ASC, id ASC) AS event_seq
    FROM scored
    WINDOW w_part AS (PARTITION BY manager_id ORDER BY rank_in_group DESC, id ASC)
)
-- Grouped metrics
SELECT
    CASE WHEN event_seq >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_tier,
    dept_id,
    ROUND(AVG(next_salary_percent_rank), 2) AS avg_next_salary_percent_rank,
    ROUND(AVG(salary_cume_dist), 2) AS avg_salary_cume_dist,
    ROUND(AVG(is_remote_flag_percent_rank), 2) AS avg_is_remote_flag_percent_rank,
    ROUND(SUM(is_remote_flag_cume_dist) / NULLIF(COUNT(CASE WHEN job_title = 'Engineer' THEN 1 END), 0), 2) AS is_remote_flag_cume_dist_per_engineer,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list
FROM metrics
GROUP BY GROUPING SETS ((CASE WHEN event_seq >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, dept_id), (CASE WHEN event_seq >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END))
