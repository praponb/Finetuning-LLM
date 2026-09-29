WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        dept_id,
        salary,
        hire_date,
        is_remote,
        level,
        INITCAP(TRIM(email)) AS email_title,
        CAST(is_remote AS INT) AS is_remote_flag,
        COALESCE(level, 'N/A') AS level_filled
    FROM employees
    WHERE hire_date > DATE '2023-07-08'
),
joined AS (
    -- Attach departments attributes
    SELECT e.*, b.location
    FROM valid e
    LEFT JOIN departments b
        ON e.dept_id = b.id
),
with_dims AS (
    -- Attach departments attributes
    SELECT a.*, b.dept_name
    FROM joined a
    LEFT JOIN departments b
        ON a.dept_id = b.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        full_name,
        salary,
        hire_date,
        level,
        is_remote_flag,
        dept_name,
        ROUND(salary / SUM(salary) OVER (PARTITION BY location), 4) AS salary_share,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY hire_date, id) AS row_seq
    FROM with_dims
    WHERE hire_date BETWEEN '2023-08-01' AND '2025-07-19'
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
    dept_name,
    SUM(salary) AS total_salary,
    SIZE(COLLECT_SET(dept_name)) AS num_dept_names,
    MAX(salary) FILTER (WHERE level = 'L4') AS l4_max_salary,
    COUNT(CASE WHEN level = 'L3' THEN 1 END) AS l3_count,
    SUM(CASE WHEN dept_name = 'HR' THEN salary_share ELSE 0 END) AS hr_salary_share,
    SUM(CASE WHEN dept_name = 'Sales' THEN salary ELSE 0 END) AS sales_salary
FROM with_windows
GROUP BY quarter_start, dept_name
ORDER BY hr_salary_share;
