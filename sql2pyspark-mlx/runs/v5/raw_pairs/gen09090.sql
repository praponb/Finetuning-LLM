WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM employees
    QUALIFY ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date DESC, id) = 1
),
enriched AS (
    -- Attach departments attributes
    SELECT
        a.*,
        d.location,
        d.budget
    FROM ranked a
    LEFT JOIN departments d
        ON a.dept_id = d.id
    WHERE d.location NOT IN ('Berlin', 'Tokyo', 'London')
),
joined AS (
    -- Attach departments attributes
    SELECT
        s.*,
        d.location AS department_location,
        d.dept_name
    FROM enriched s
    LEFT JOIN departments d
        ON s.dept_id = d.id
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) = 1
),
enriched_employees AS (
    -- Attach departments attributes
    SELECT
        src.*,
        p.dept_name AS department_dept_name,
        p.location AS department_location_2,
        p.budget AS department_budget
    FROM deduped src
    LEFT JOIN departments p
        ON src.dept_id = p.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN department_budget >= 3599000 THEN 'PREMIUM' ELSE 'STANDARD' END AS department_budget_tier,
    AVG(department_budget) FILTER (WHERE location = 'New York') AS new_york_avg_department_budget,
    ROUND(SUM(budget) / NULLIF(COUNT(CASE WHEN job_title = 'Manager' THEN 1 END), 0), 2) AS budget_per_manager,
    COUNT(CASE WHEN location = 'Tokyo' THEN 1 END) AS tokyo_count
FROM enriched_employees
GROUP BY 1
ORDER BY new_york_avg_department_budget DESC NULLS FIRST
