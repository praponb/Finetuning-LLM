WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(full_name) full_name_upper,
        CEIL(salary) salary_whole,
        COALESCE(level, 'OTHER') level_clean
    FROM employees
    WHERE hire_date >= DATE '2023-05-22'
      AND salary >= 86000
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary, id) AS rank_in_group
    FROM base
    QUALIFY rank_in_group = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        full_name,
        dept_id,
        salary,
        hire_date,
        is_remote,
        full_name_upper,
        level_clean,
        rank_in_group,
        SUM(salary_whole) OVER (PARTITION BY manager_id, job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_salary_whole,
        MIN(salary_whole) OVER (PARTITION BY manager_id, job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_salary_whole,
        MAX(salary_whole) OVER (PARTITION BY manager_id, job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_salary_whole
    FROM ranked
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_id,
        is_remote,
        running_min_salary_whole,
        running_max_salary_whole,
        STDDEV_SAMP(running_max_salary_whole) OVER (PARTITION BY dept_id) dept_id_stddev_samp_running_max_salary_whole,
        AVG(running_max_salary_whole) OVER (ORDER BY rank_in_group, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) running_max_salary_whole_avg_3
    FROM metrics
),
enriched AS (
    -- Attach departments attributes
    SELECT
        src.id,
        src.dept_id,
        src.is_remote,
        src.running_max_salary_whole,
        src.dept_id_stddev_samp_running_max_salary_whole,
        src.running_max_salary_whole_avg_3,
        d.location,
        d.budget
    FROM with_windows src
    LEFT JOIN departments d
        ON src.dept_id = d.id
)
-- Grouped metrics
SELECT
    CASE WHEN dept_id_stddev_samp_running_max_salary_whole >= 79 THEN 'HIGH' ELSE 'LOW' END AS dept_id_stddev_samp_running_max_salary_whole_tier,
    dept_id,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(running_max_salary_whole_avg_3, ARRAY(0.25, 0.5, 0.75)) AS running_max_salary_whole_avg_3_quartiles,
    APPROX_PERCENTILE(running_max_salary_whole_avg_3, ARRAY(0.25, 0.5, 0.75)) AS running_max_salary_whole_avg_3_quartiles_2,
    PERCENTILE_APPROX(dept_id_stddev_samp_running_max_salary_whole, ARRAY(0.25, 0.5, 0.75)) AS dept_id_stddev_samp_running_max_salary_whole_quartiles
FROM enriched
GROUP BY CASE WHEN dept_id_stddev_samp_running_max_salary_whole >= 79 THEN 'HIGH' ELSE 'LOW' END, dept_id
HAVING distinct_ids < 1000
ORDER BY dept_id_stddev_samp_running_max_salary_whole_tier NULLS LAST;
