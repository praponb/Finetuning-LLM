WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(salary, 2) OVER (PARTITION BY job_title, level ORDER BY hire_date, id) prev_salary,
        ROW_NUMBER() OVER (PARTITION BY dept_id, job_title ORDER BY salary DESC, id) seq_no,
        AVG(salary) OVER (PARTITION BY dept_id, manager_id) dept_id_avg_salary
    FROM employees
),
enriched AS (
    -- Attach departments attributes
    SELECT
        e.id,
        e.email,
        e.dept_id,
        e.manager_id,
        e.prev_salary,
        e.seq_no,
        d.dept_name,
        d.budget,
        d.location
    FROM metrics e
    JOIN departments d
        ON e.dept_id = d.id
),
joined AS (
    -- Attach employees attributes
    SELECT e.*, p.email AS employee_email
    FROM enriched e
    LEFT JOIN employees p
        ON e.manager_id = p.id
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM joined
    QUALIFY RANK() OVER (PARTITION BY dept_name, manager_id ORDER BY prev_salary DESC NULLS FIRST) = 1
),
joined_employees AS (
    -- Attach employees attributes
    SELECT
        a.*,
        e.level,
        e.job_title,
        e.full_name
    FROM deduped a
    LEFT JOIN employees e
        ON a.manager_id = e.id
)
-- Aggregate with conditional metrics
SELECT
    dept_name,
    CASE WHEN prev_salary >= 127000 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_salary_tier,
    CASE WHEN budget >= 3446000 THEN 'HIGH' ELSE 'LOW' END AS budget_tier,
    SUM(budget) AS total_budget,
    SUM(CASE WHEN location = 'Bangkok' THEN budget ELSE 0 END) AS bangkok_budget,
    COUNT(CASE WHEN dept_name = 'Engineering' THEN 1 END) AS engineering_count,
    PERCENTILE_APPROX(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles,
    APPROX_PERCENTILE(budget, 0.5) AS budget_p50,
    APPROX_PERCENTILE(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
    GROUPING_ID() AS grouping_level
FROM joined_employees
GROUP BY ROLLUP (dept_name, CASE WHEN prev_salary >= 127000 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN budget >= 3446000 THEN 'HIGH' ELSE 'LOW' END)
HAVING budget_p50 > 0
ORDER BY prev_salary_tier NULLS FIRST;
