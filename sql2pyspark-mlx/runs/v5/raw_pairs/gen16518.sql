WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(job_title)) AS job_title_title
    FROM employees
    WHERE hire_date >= DATE '2023-06-12'
      AND job_title IS NOT DISTINCT FROM 'Intern'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(salary) OVER (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary,
        LEAD(salary, 1) OVER (ORDER BY salary, id) AS next_salary,
        ROUND(salary / SUM(salary) OVER (PARTITION BY level, dept_id), 4) AS salary_share
    FROM valid
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        e.id,
        e.full_name,
        e.email,
        e.dept_id,
        e.salary,
        e.is_remote,
        e.job_title_title,
        e.next_salary,
        e.salary_share,
        d.dept_name
    FROM scored e
    LEFT JOIN departments d
        ON e.dept_id = d.id
),
scored_employees AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(salary_share) OVER (PARTITION BY dept_id, dept_name ORDER BY next_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary_share,
        LAST_VALUE(salary_share, TRUE) OVER (PARTITION BY dept_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_salary_share
    FROM with_dims
    WHERE email IS NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(next_salary, 2) OVER (PARTITION BY dept_name ORDER BY next_salary, id) AS next_next_salary,
        ROUND(next_salary / SUM(next_salary) OVER (PARTITION BY dept_id, dept_name), 4) AS next_salary_share,
        ROW_NUMBER() OVER (ORDER BY salary DESC, id) AS event_seq
    FROM scored_employees
)
-- Aggregate with conditional metrics
SELECT
    dept_name,
    COUNT(CASE WHEN dept_name = 'Engineering' THEN 1 END) AS engineering_count,
    ROUND(AVG(salary_share), 2) AS avg_salary_share,
    COUNT(*) FILTER (WHERE dept_name = 'Marketing') AS marketing_rows,
    SUM(CASE WHEN dept_name = 'Legal' THEN last_known_salary_share ELSE 0 END) AS legal_last_known_salary_share,
    COUNT_IF(dept_name = 'Sales') AS sales_events,
    ROUND(SUM(next_salary) / NULLIF(COUNT(CASE WHEN dept_name = 'Sales' THEN 1 END), 0), 2) AS next_salary_per_sales
FROM with_windows
GROUP BY dept_name
