WITH with_latest AS (
    SELECT
        s.id,
        s.dept_name,
        s.budget,
        x.employee_hire_date
    FROM departments s
    JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_last AS (
    SELECT
        s.id,
        s.dept_name,
        s.employee_hire_date,
        top1.level
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest_departments AS (
    SELECT
        s.id,
        s.dept_name,
        s.employee_hire_date,
        s.level,
        x.employee_job_title
    FROM with_last s
    JOIN LATERAL (
            SELECT c.job_title AS employee_job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest_match AS (
    SELECT
        s.id,
        s.dept_name,
        s.employee_hire_date,
        s.level,
        s.employee_job_title,
        latest.employee_job_title_2
    FROM with_latest_departments s
    LEFT JOIN LATERAL (
            SELECT c.job_title AS employee_job_title_2
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    DATE_TRUNC('MONTH', employee_hire_date) AS activity_month,
    DATE_TRUNC('QUARTER', employee_hire_date) AS quarter_start,
    employee_job_title,
    COUNT(CASE WHEN employee_job_title = 'Director' THEN 1 END) AS director_count,
    COUNT(DISTINCT employee_job_title) AS distinct_employee_job_titles,
    SORT_ARRAY(COLLECT_SET(employee_job_title)) AS employee_job_title_list,
    COUNT(*) AS cnt,
    COUNT(DISTINCT employee_job_title) AS distinct_employee_job_titles_2,
    COUNT(*) AS event_count
FROM latest_match
GROUP BY ROLLUP (DATE_TRUNC('MONTH', employee_hire_date), DATE_TRUNC('QUARTER', employee_hire_date), employee_job_title)
ORDER BY employee_job_title ASC;
