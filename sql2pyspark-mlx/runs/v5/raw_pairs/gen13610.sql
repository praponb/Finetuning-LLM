WITH filtered AS (
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        REPLACE(email, ' ', '-') AS email_slug,
        CAST(is_remote AS INT) AS is_remote_flag,
        COALESCE(job_title, 'UNKNOWN') AS job_title_clean
    FROM employees
    WHERE level IS NOT NULL
),
joined AS (
    SELECT
        e.id,
        e.full_name,
        e.manager_id,
        e.salary,
        e.hire_date,
        e.is_remote,
        e.email_slug,
        p2.job_title AS employee_job_title,
        p2.hire_date AS employee_hire_date,
        p2.salary AS employee_salary
    FROM filtered e
    JOIN employees p2
        ON e.manager_id = p2.id
    WHERE UPPER(TRIM(p2.email)) = 'LIAM@OUTLOOK.COM'
),
metrics AS (
    SELECT
        *,
        LEAD(employee_salary, 1, 0) OVER (PARTITION BY employee_job_title ORDER BY employee_hire_date, id) AS next_employee_salary,
        MAX(salary) OVER (PARTITION BY employee_job_title) AS employee_job_title_max_salary
    FROM joined
),
scored AS (
    SELECT
        id,
        salary,
        is_remote,
        employee_salary,
        next_employee_salary,
        employee_job_title_max_salary,
        MIN(employee_salary) OVER (PARTITION BY manager_id, employee_job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_employee_salary,
        AVG(employee_salary) OVER (PARTITION BY manager_id, employee_job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_employee_salary,
        SUM(employee_salary) OVER (PARTITION BY manager_id, employee_job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_employee_salary,
        MAX(employee_salary) OVER (PARTITION BY manager_id, employee_job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_employee_salary
    FROM metrics
)
SELECT
    CASE WHEN salary >= 121000 THEN 'LARGE' ELSE 'SMALL' END AS salary_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    APPROX_PERCENTILE(next_employee_salary, 0.95) AS next_employee_salary_p95,
    COUNT(*) AS cnt,
    APPROX_PERCENTILE(salary, 0.5) AS salary_p50
FROM scored
GROUP BY CASE WHEN salary >= 121000 THEN 'LARGE' ELSE 'SMALL' END
HAVING COUNT(DISTINCT id) > 1
