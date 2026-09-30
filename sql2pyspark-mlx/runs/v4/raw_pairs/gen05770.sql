WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget ASC, id ASC) AS rank_in_group
    FROM departments
    QUALIFY rank_in_group = 1
),
latest_match AS (
    SELECT
        s.*,
        top1.employee_salary,
        top1.hire_date
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.salary AS employee_salary, c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
ranked_departments AS (
    SELECT *, RANK() OVER (PARTITION BY dept_name ORDER BY employee_salary ASC NULLS LAST) AS latest_rank
    FROM latest_match
    QUALIFY latest_rank = 1
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY hire_date ASC NULLS LAST, id ASC) AS rn
        FROM ranked_departments
    ) t
    WHERE rn = 1
)
SELECT
    DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
    DATE_TRUNC('WEEK', hire_date) AS week_start,
    COUNT(DISTINCT dept_name) AS distinct_dept_names,
    SIZE(COLLECT_SET(location)) AS num_locations,
    ROUND(AVG(budget), 2) AS avg_budget,
    APPROX_PERCENTILE(employee_salary, ARRAY(0.25, 0.5, 0.75)) AS employee_salary_quartiles,
    ROUND(AVG(employee_salary), 2) AS avg_employee_salary,
    SUM(CASE WHEN dept_name = 'Marketing' THEN budget ELSE 0 END) AS marketing_budget
FROM deduped
GROUP BY quarter_start, week_start
ORDER BY avg_budget ASC NULLS LAST
