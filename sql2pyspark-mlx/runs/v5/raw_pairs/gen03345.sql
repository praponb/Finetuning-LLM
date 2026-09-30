WITH cleaned AS (
    SELECT *, UPPER(job_title) AS job_title_upper
    FROM employees
    WHERE hire_date > DATE '2023-03-28'
      AND job_title IS NOT NULL
),
enriched AS (
    SELECT
        e.*,
        b.salary AS employee_salary,
        b.full_name AS employee_full_name,
        b.hire_date AS employee_hire_date
    FROM cleaned e
    LEFT JOIN employees b
        ON e.manager_id = b.id
),
scored AS (
    SELECT
        *,
        ROUND(employee_salary / SUM(employee_salary) OVER (PARTITION BY job_title, manager_id), 4) AS employee_salary_share,
        AVG(salary) OVER (PARTITION BY dept_id, level ORDER BY hire_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS salary_avg_3_prior,
        ROW_NUMBER() OVER (ORDER BY hire_date, id) AS row_seq,
        AVG(salary) OVER (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS salary_avg_5_prior,
        LAST_VALUE(job_title, TRUE) OVER (ORDER BY employee_hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_job_title
    FROM enriched
),
scored_employees AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        job_title_upper,
        employee_salary,
        employee_hire_date,
        employee_salary_share,
        row_seq,
        STDDEV_SAMP(row_seq) OVER (PARTITION BY level, job_title) AS level_stddev_samp_row_seq,
        AVG(salary_avg_3_prior) OVER (PARTITION BY job_title ORDER BY employee_salary_share, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS salary_avg_3_prior_avg_2_prior,
        SUM(salary_avg_5_prior) OVER (PARTITION BY manager_id) AS manager_id_sum_salary_avg_5_prior
    FROM scored
    WHERE hire_date > '2023-07-17'
)
SELECT
    id,
    full_name,
    email,
    salary,
    hire_date,
    level,
    job_title_upper,
    employee_salary,
    employee_salary_share,
    CASE WHEN level RLIKE '^(L5|L1)$' THEN 'EXTENDED' WHEN level RLIKE '^(L2|L3)$' THEN 'GROUP_A' ELSE 'CORE' END AS level_zone
FROM scored_employees
