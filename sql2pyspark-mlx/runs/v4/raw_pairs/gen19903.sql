WITH latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.level,
        top1.job_title
    FROM departments s
    JOIN LATERAL (
            SELECT c.level, c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    dept_name,
    location,
    budget,
    level,
    job_title,
    CASE WHEN dept_name RLIKE '^(Finance|Sales)$' THEN 'GROUP_B' WHEN dept_name RLIKE '^(Engineering|Marketing)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS dept_name_zone
FROM latest_match
ORDER BY job_title DESC
