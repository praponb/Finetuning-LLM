WITH valid AS (
    SELECT
        *,
        LENGTH(dept_name) AS dept_name_length,
        COALESCE(location, 'OTHER') AS location_clean
    FROM departments
    WHERE location IS NOT NULL
),
with_latest AS (
    SELECT s.*, top1.employee_job_title
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.job_title AS employee_job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        dept_name_length,
        location_clean,
        employee_job_title,
        NTILE(10) OVER (ORDER BY dept_name_length DESC, id) AS dept_name_length_bucket,
        LAST_VALUE(location) IGNORE NULLS OVER (ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_location,
        LAST_VALUE(dept_name_length, TRUE) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_dept_name_length
    FROM with_latest
)
SELECT
    id,
    location,
    budget,
    location_clean,
    dept_name_length_bucket,
    last_known_location,
    last_known_dept_name_length
FROM enriched
ORDER BY location, last_known_dept_name_length;
