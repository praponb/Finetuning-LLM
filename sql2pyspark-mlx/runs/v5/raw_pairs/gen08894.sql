WITH with_dims AS (
    -- Attach employees attributes
    SELECT
        s.*,
        e.level AS employee_level,
        e.full_name AS employee_full_name
    FROM employees s
    INNER JOIN employees e
        ON s.manager_id = e.id
),
with_dims_employees AS (
    -- Attach departments attributes
    SELECT
        src.id,
        src.full_name,
        src.email,
        src.dept_id,
        src.salary,
        src.hire_date,
        src.is_remote,
        src.employee_level,
        src.employee_full_name,
        d.budget,
        d.location,
        d.dept_name
    FROM with_dims src
    LEFT JOIN departments d
        ON src.dept_id = d.id
    WHERE d.dept_name IN ('Legal', 'Marketing', 'Sales')
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(location, TRUE) OVER (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_location,
        LAG(budget, 2) OVER (ORDER BY budget, id) AS prev_budget,
        CUME_DIST() OVER (PARTITION BY employee_level ORDER BY budget DESC NULLS LAST) AS budget_cume_dist
    FROM with_dims_employees
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, dept_id
    FROM enriched
    INTERSECT ALL
    SELECT id, dept_id
    FROM employees
    WHERE is_remote
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.id = k.id AND s.dept_id = k.dept_id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(prev_budget / SUM(prev_budget) OVER (PARTITION BY dept_id), 4) AS prev_budget_share,
        RANK() OVER (PARTITION BY location ORDER BY budget_cume_dist DESC) AS budget_cume_dist_rank,
        COUNT(prev_budget) OVER (PARTITION BY location) AS location_count_prev_budget,
        ROUND(salary / SUM(salary) OVER (PARTITION BY employee_level, dept_name), 4) AS salary_share
    FROM kept_rows
),
metrics_employees AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER w_grp AS budget_cume_dist_rank_2,
        RANK() OVER (ORDER BY salary) AS salary_rank,
        PERCENT_RANK() OVER (ORDER BY budget NULLS LAST) AS budget_percent_rank
    FROM metrics
    WINDOW w_grp AS (PARTITION BY employee_level ORDER BY budget_cume_dist NULLS LAST)
),
with_dims_employees_2 AS (
    -- Attach departments attributes
    SELECT
        a.id,
        a.full_name,
        a.email,
        a.dept_id,
        a.salary,
        a.hire_date,
        a.is_remote,
        a.employee_level,
        a.employee_full_name,
        a.budget,
        a.dept_name,
        a.prev_budget_share,
        a.budget_cume_dist_rank,
        a.location_count_prev_budget,
        a.salary_share,
        a.salary_rank,
        a.budget_percent_rank,
        p.dept_name AS department_dept_name,
        p.budget AS department_budget
    FROM metrics_employees a
    LEFT JOIN departments p
        ON a.dept_id = p.id
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            dept_id,
            dept_name,
            department_dept_name,
            salary
        FROM with_dims_employees_2
    ) src
    PIVOT (
        AVG(salary) AS amount
        FOR department_dept_name IN ('Marketing', 'Sales')
    )
)
SELECT
    s.dept_id,
    s.dept_name,
    s.Sales,
    CASE WHEN dept_name RLIKE '^(HR|Finance)$' THEN 'GROUP_B' WHEN dept_name RLIKE '^(Engineering|Sales)$' THEN 'CORE' ELSE 'PRIORITY' END AS dept_name_zone,
    (SELECT AVG(t2.salary) FROM employees t2 WHERE t2.dept_id = s.dept_id) AS avg_employee_salary
FROM pivoted s
ORDER BY avg_employee_salary DESC NULLS LAST, dept_name DESC;
