WITH base AS (
    SELECT
        *,
        SPLIT(LOWER(email), '@')[0] AS email_user,
        COALESCE(job_title, 'UNKNOWN') AS job_title_clean
    FROM employees
    WHERE hire_date > DATE '2023-09-03'
      AND is_remote
),
joined AS (
    SELECT a.*, p.budget
    FROM base a
    LEFT JOIN departments p
        ON a.dept_id = p.id
    WHERE p.budget BETWEEN 1641000 AND 2242000
),
joined_employees AS (
    SELECT
        s.id,
        s.full_name,
        s.dept_id,
        s.job_title,
        s.level,
        s.budget,
        d.location,
        d.budget AS department_budget,
        d.dept_name
    FROM joined s
    LEFT JOIN departments d
        ON s.dept_id = d.id
),
scored AS (
    SELECT
        id,
        full_name,
        level,
        dept_name,
        SUM(department_budget) OVER (PARTITION BY level ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_department_budget,
        SUM(department_budget) OVER (PARTITION BY dept_id, job_title ORDER BY department_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_department_budget
    FROM joined_employees
),
deduped AS (
    SELECT *
    FROM scored
    QUALIFY RANK() OVER (PARTITION BY level ORDER BY running_department_budget DESC) = 1
),
grouped AS (
    SELECT
        level,
        ROUND(AVG(running_department_budget), 2) AS avg_running_department_budget,
        COUNT_IF(dept_name = 'Engineering') AS engineering_events,
        ROUND(AVG(running_department_budget), 2) AS avg_running_department_budget_2
    FROM deduped
    GROUP BY level
)
SELECT
    level,
    avg_running_department_budget,
    engineering_events,
    avg_running_department_budget_2,
    CASE WHEN level RLIKE '^(L4|L2)$' THEN 'GROUP_A' WHEN level RLIKE '^(L1|L3)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS level_zone
FROM grouped
ORDER BY avg_running_department_budget NULLS LAST
