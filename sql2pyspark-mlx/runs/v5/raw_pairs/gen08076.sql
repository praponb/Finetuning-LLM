WITH latest_match AS (
    SELECT
        s.*,
        top1.job_title,
        top1.salary
    FROM departments s
    LEFT JOIN LATERAL (
            SELECT c.job_title, c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest AS (
    SELECT
        s.id,
        s.dept_name,
        latest.employee_hire_date
    FROM latest_match s
    JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY employee_hire_date, id) AS rn
    FROM with_latest
    QUALIFY rn = 1
),
base AS (
    SELECT *, DAYOFWEEK(employee_hire_date) employee_hire_date_dayofweek
    FROM deduped
    WHERE dept_name IS NOT NULL
      AND employee_hire_date BETWEEN '2024-07-27' AND '2024-10-01'
),
deduped_departments AS (
    SELECT
        id,
        employee_hire_date,
        rn
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY employee_hire_date_dayofweek, id) = 1
)
SELECT id, employee_hire_date
FROM deduped_departments
