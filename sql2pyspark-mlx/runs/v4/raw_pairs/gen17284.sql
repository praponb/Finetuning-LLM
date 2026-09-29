WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(job_title) AS job_title_lower,
        INITCAP(TRIM(email)) AS email_title,
        COALESCE(job_title, 'NONE') AS resolved_job_title
    FROM employees
    WHERE hire_date >= DATE '2024-03-05'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(salary, 1, 0) OVER (PARTITION BY job_title ORDER BY hire_date, id) AS prev_salary,
        LAG(salary, 1) OVER (PARTITION BY dept_id ORDER BY salary, id) AS prev_salary_2
    FROM valid
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(prev_salary_2) OVER w_recent AS running_prev_salary_2,
        AVG(prev_salary) OVER (ORDER BY prev_salary_2, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS prev_salary_avg_2,
        ROW_NUMBER() OVER w_grp AS row_seq,
        SUM(prev_salary_2) OVER (PARTITION BY dept_id ORDER BY prev_salary_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_salary_2
    FROM enriched
    WINDOW w_recent AS (PARTITION BY job_title ORDER BY prev_salary_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY job_title ORDER BY prev_salary DESC, id)
)
SELECT
    s.id,
    s.full_name,
    s.manager_id,
    s.job_title,
    s.salary,
    s.is_remote,
    s.level,
    s.job_title_lower,
    s.email_title,
    (SELECT MIN(e2.salary) FROM employees e2 WHERE e2.manager_id = s.manager_id) AS min_employee_salary
FROM scored s
ORDER BY level DESC NULLS FIRST;
