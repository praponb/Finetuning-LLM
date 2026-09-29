WITH with_last AS (
    SELECT
        s.id,
        s.dept_name,
        top1.employee_hire_date,
        top1.job_title
    FROM departments s
    LEFT JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date, c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title, dept_name ORDER BY employee_hire_date DESC NULLS FIRST, id ASC) AS rank_in_group
    FROM with_last
    QUALIFY rank_in_group = 1
),
ranked_departments AS (
    SELECT
        id,
        employee_hire_date,
        job_title,
        rank_in_group
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY rank_in_group DESC, id ASC) AS dedup_rank
        FROM ranked
    ) ranked
    WHERE dedup_rank = 1
),
latest_match AS (
    SELECT
        s.*,
        top1.employee_level,
        top1.employee_job_title
    FROM ranked_departments s
    LEFT JOIN LATERAL (
            SELECT c.level AS employee_level, c.job_title AS employee_job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    job_title,
    employee_level,
    employee_job_title
FROM latest_match
ORDER BY employee_level DESC NULLS LAST
