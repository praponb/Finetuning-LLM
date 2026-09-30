WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_name,
        location,
        budget,
        RANK() OVER (ORDER BY budget NULLS LAST) budget_rank,
        AVG(budget) OVER w_ordered budget_avg_3,
        PERCENT_RANK() OVER (PARTITION BY dept_name ORDER BY budget DESC NULLS LAST) budget_percent_rank
    FROM departments
    WHERE budget < 1192000
    WINDOW w_ordered AS (PARTITION BY location, dept_name ORDER BY budget, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.budget_rank,
        s.budget_avg_3,
        s.budget_percent_rank,
        x.job_title,
        x.hire_date
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.job_title, c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.hire_date <= s.hire_date
        )
),
latest_match_departments AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.budget_rank,
        s.budget_avg_3,
        s.budget_percent_rank,
        s.job_title,
        s.hire_date,
        x.employee_level,
        x.employee_job_title
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.level AS employee_level, c.job_title AS employee_job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Multi-level aggregation
SELECT
    employee_job_title,
    COUNT(*) FILTER (WHERE job_title = 'Director') AS director_rows,
    SORT_ARRAY(COLLECT_SET(employee_level)) AS employee_level_list
FROM latest_match_departments
GROUP BY employee_job_title;
