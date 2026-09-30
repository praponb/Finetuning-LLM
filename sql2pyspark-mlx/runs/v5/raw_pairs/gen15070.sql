WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(email, '/', full_name) AS email_full_name,
        LENGTH(level) AS level_length
    FROM employees
    WHERE hire_date > DATE '2023-09-06'
),
matched_keys AS (
    -- Set difference on keys
    SELECT dept_id, job_title
    FROM valid
    EXCEPT
    SELECT dept_id, job_title
    FROM employees
    WHERE email LIKE '%@startup.io'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.dept_id = k.dept_id AND s.job_title = k.job_title
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        src.*,
        d.level AS employee_level,
        d.hire_date AS employee_hire_date,
        d.job_title AS employee_job_title
    FROM kept_rows src
    LEFT JOIN employees d
        ON src.manager_id = d.id
),
with_dims_employees AS (
    -- Attach departments attributes
    SELECT
        s.email,
        s.dept_id,
        s.job_title,
        s.hire_date,
        s.is_remote,
        s.level,
        s.employee_level,
        d.budget,
        d.dept_name,
        d.location
    FROM with_dims s
    INNER JOIN departments d
        ON s.dept_id = d.id
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN budget >= 4087000 THEN 'LARGE' ELSE 'SMALL' END AS budget_tier,
        APPROX_PERCENTILE(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
        APPROX_PERCENTILE(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles_2,
        SUM(budget) AS total_budget,
        SORT_ARRAY(COLLECT_SET(location)) AS location_list
    FROM with_dims_employees
    GROUP BY CASE WHEN budget >= 4087000 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    budget_tier,
    budget_quartiles,
    budget_quartiles_2,
    location_list
FROM totals
ORDER BY budget_tier NULLS LAST
LIMIT 10
