WITH base AS (
    SELECT
        *,
        CASE WHEN dept_name = 'Marketing' THEN 1 ELSE 0 END AS is_marketing,
        COALESCE(dept_name, 'OTHER') AS dept_name_filled
    FROM departments
    WHERE location IS NOT NULL
),
latest_match AS (
    SELECT
        s.id,
        s.dept_name_filled,
        latest.salary
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest_match_departments AS (
    SELECT
        s.*,
        top1.employee_job_title,
        top1.employee_salary
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.job_title AS employee_job_title, c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest AS (
    SELECT *
    FROM latest_match_departments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_job_title ORDER BY employee_salary DESC, id) = 1
),
metrics AS (
    SELECT
        id,
        salary,
        employee_salary,
        CUME_DIST() OVER (PARTITION BY employee_job_title ORDER BY salary) AS salary_cume_dist,
        FIRST_VALUE(employee_job_title, TRUE) OVER w_hist AS first_known_employee_job_title,
        NTILE(4) OVER (PARTITION BY employee_job_title ORDER BY employee_salary, id) AS employee_salary_bucket,
        SUM(salary) OVER (PARTITION BY employee_job_title ORDER BY employee_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary,
        ROW_NUMBER() OVER (PARTITION BY employee_job_title ORDER BY salary DESC, id) AS event_seq
    FROM latest
    WINDOW w_hist AS (PARTITION BY employee_job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY salary, id) AS seq_no,
        AVG(salary) OVER (ORDER BY cumulative_salary, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS salary_avg_2,
        LAST_VALUE(cumulative_salary, TRUE) OVER (ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cumulative_salary,
        LAG(salary_cume_dist, 2) OVER (ORDER BY event_seq, id) AS prev_salary_cume_dist,
        LAG(salary, 2) OVER (ORDER BY salary, id) AS prev_salary
    FROM metrics
)
SELECT
    id,
    salary,
    employee_salary_bucket,
    cumulative_salary,
    event_seq,
    salary_avg_2,
    last_known_cumulative_salary,
    prev_salary_cume_dist,
    prev_salary
FROM scored
ORDER BY salary_avg_2 DESC
