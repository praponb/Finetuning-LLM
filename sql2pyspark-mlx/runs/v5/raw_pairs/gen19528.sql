WITH with_dims AS (
    -- Attach departments attributes
    SELECT
        e.*,
        d.location,
        d.dept_name
    FROM employees e
    LEFT JOIN departments d
        ON e.dept_id = d.id
    WHERE d.location <> 'Singapore'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_id,
        manager_id,
        salary,
        location,
        dept_name,
        MIN(salary) OVER (PARTITION BY level, location) AS level_min_salary,
        FIRST_VALUE(job_title) IGNORE NULLS OVER (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_job_title,
        NTILE(4) OVER (PARTITION BY manager_id ORDER BY salary DESC, id) AS salary_bucket
    FROM with_dims
),
remaining_keys AS (
    -- Set difference on keys
    SELECT manager_id, id
    FROM enriched
    EXCEPT ALL
    SELECT manager_id, id
    FROM employees
    WHERE level IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.manager_id = k.manager_id AND s.id = k.id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(location, 'N/A') AS location_filled
    FROM verified
    WHERE dept_name IS DISTINCT FROM 'HR'
      AND manager_id IS NOT NULL
),
enriched_employees AS (
    -- Attach employees attributes
    SELECT
        a.id,
        a.dept_id,
        a.manager_id,
        a.salary,
        a.level_min_salary,
        a.first_known_job_title,
        a.location_filled,
        p.salary AS employee_salary,
        p.email
    FROM cleaned a
    INNER JOIN employees p
        ON a.manager_id = p.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN level_min_salary >= 153052 THEN 'LARGE' ELSE 'SMALL' END AS level_min_salary_tier,
    PERCENTILE_APPROX(salary, ARRAY(0.25, 0.5, 0.75)) AS salary_quartiles,
    MAX(level_min_salary) AS max_level_min_salary,
    ROUND(AVG(employee_salary), 2) AS avg_employee_salary
FROM enriched_employees
GROUP BY level_min_salary_tier
HAVING max_level_min_salary > 0;
