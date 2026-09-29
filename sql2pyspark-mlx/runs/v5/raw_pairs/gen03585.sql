WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(full_name) AS full_name_lower,
        COALESCE(job_title, 'UNKNOWN') AS job_title_filled
    FROM employees
    WHERE hire_date < '2024-12-01'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(salary) OVER (PARTITION BY manager_id, dept_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_salary,
        MAX(salary) OVER (PARTITION BY manager_id, dept_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_salary,
        SUM(salary) OVER (PARTITION BY manager_id, dept_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_salary
    FROM cleaned
    WHERE email LIKE '%@outlook.com'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_id,
        manager_id,
        job_title,
        salary,
        is_remote,
        full_name_lower,
        running_max_salary,
        running_sum_salary,
        AVG(running_max_salary) OVER w_grp AS running_max_salary_avg_5_prior,
        AVG(running_min_salary) OVER (PARTITION BY job_title, level ORDER BY running_sum_salary, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS running_min_salary_avg_6
    FROM enriched
    WINDOW w_grp AS (PARTITION BY dept_id ORDER BY hire_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
),
scored_employees AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        manager_id,
        is_remote,
        full_name_lower,
        running_sum_salary,
        running_max_salary_avg_5_prior,
        STDDEV_SAMP(running_max_salary) OVER (PARTITION BY manager_id) AS manager_id_stddev_samp_running_max_salary,
        SUM(running_sum_salary) OVER w_part AS running_running_sum_salary,
        AVG(running_min_salary_avg_6) OVER (PARTITION BY job_title ORDER BY running_min_salary_avg_6, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS running_min_salary_avg_6_avg_6
    FROM scored
    WINDOW w_part AS (PARTITION BY job_title, dept_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    is_remote,
    running_sum_salary,
    running_max_salary_avg_5_prior,
    running_running_sum_salary
FROM scored_employees
ORDER BY id DESC
LIMIT 20;
