WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(salary * 0.9, 2) AS salary_scaled
    FROM employees
    WHERE level IS DISTINCT FROM 'L5'
      AND email LIKE '%@gmail.com'
),
joined AS (
    -- Attach departments attributes
    SELECT
        a.id,
        a.full_name,
        a.email,
        a.dept_id,
        a.manager_id,
        a.job_title,
        a.salary,
        a.hire_date,
        a.level,
        p.dept_name,
        p.location,
        p.budget
    FROM base a
    LEFT JOIN departments p
        ON a.dept_id = p.id
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title, dept_id ORDER BY salary DESC, id) AS rn
    FROM joined
    QUALIFY rn = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        manager_id,
        job_title,
        salary,
        hire_date,
        dept_name,
        location,
        AVG(salary) OVER w_running AS running_avg_salary,
        COUNT(salary) OVER w_running AS running_count_salary,
        SUM(salary) OVER w_running AS running_sum_salary
    FROM latest
    WINDOW w_running AS (PARTITION BY location ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        a.*,
        d.full_name,
        d.hire_date AS employee_hire_date,
        d.level
    FROM enriched a
    LEFT JOIN employees d
        ON a.manager_id = d.id
    WHERE d.hire_date < '2024-03-23'
),
latest_employees AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        salary,
        location,
        running_avg_salary,
        running_count_salary,
        running_sum_salary,
        employee_hire_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY running_count_salary DESC, id) AS rn
        FROM with_dims
    ) t
    WHERE rn = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        location,
        running_count_salary,
        running_sum_salary,
        LEAD(salary, 1, 0) OVER (PARTITION BY location ORDER BY salary, id) AS next_salary,
        ROW_NUMBER() OVER (ORDER BY salary DESC, id) AS seq_no,
        AVG(salary) OVER (PARTITION BY location ORDER BY running_avg_salary, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS salary_avg_3_prior
    FROM latest_employees
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN running_count_salary >= 15 THEN 'LARGE' ELSE 'SMALL' END AS running_count_salary_tier,
    CASE WHEN next_salary >= 150000 THEN 'HIGH' ELSE 'LOW' END AS next_salary_tier,
    COUNT(*) AS cnt,
    COUNT_IF(location = 'Tokyo') AS tokyo_events
FROM metrics
GROUP BY running_count_salary_tier, next_salary_tier
HAVING COUNT(*) < 1000;
