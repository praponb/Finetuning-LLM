WITH valid AS (
    SELECT *, LOWER(level) AS level_lower
    FROM employees
    WHERE hire_date >= DATE '2023-04-09'
      AND level IS NOT NULL
),
enriched AS (
    SELECT
        a.id,
        a.dept_id,
        a.manager_id,
        a.salary,
        a.hire_date,
        p.dept_name,
        p.budget
    FROM valid a
    INNER JOIN departments p
        ON a.dept_id = p.id
),
with_dims AS (
    SELECT
        a.*,
        d.location,
        d.budget AS department_budget
    FROM enriched a
    LEFT JOIN departments d
        ON a.dept_id = d.id
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY department_budget ASC, id ASC) AS row_num
        FROM with_dims
    ) ranked
    WHERE row_num = 1
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY location ORDER BY salary DESC) AS dedup_rank
        FROM latest
    ) x
    WHERE dedup_rank = 1
)
SELECT
    location,
    dept_name,
    SUM(budget) AS total_budget,
    COUNT(*) FILTER (WHERE dept_name = 'HR') AS hr_rows,
    COUNT(DISTINCT dept_name) AS distinct_dept_names,
    COUNT(CASE WHEN location = 'Chiang Mai' THEN 1 END) AS chiang_mai_count
FROM deduped
GROUP BY 1, 2
HAVING COUNT(DISTINCT dept_name) > 1;
