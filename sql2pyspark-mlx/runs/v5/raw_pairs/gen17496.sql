WITH with_last AS (
    SELECT
        s.id,
        s.dept_name,
        x.hire_date,
        x.job_title
    FROM departments s
    JOIN LATERAL (
            SELECT c.hire_date, c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    SELECT
        *,
        LAST_VALUE(job_title) IGNORE NULLS OVER w_hist AS last_known_job_title,
        LAST_VALUE(dept_name) IGNORE NULLS OVER w_hist AS last_known_dept_name
    FROM with_last
    WHERE job_title = 'Manager'
    WINDOW w_hist AS (ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
eligible AS (
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.hire_date < s.hire_date
        )
),
enriched AS (
    SELECT
        id,
        hire_date,
        job_title,
        last_known_job_title,
        last_known_dept_name,
        LAST_VALUE(job_title, TRUE) OVER (PARTITION BY dept_name, job_title ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_job_title_2,
        ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date DESC, id) AS event_seq
    FROM eligible
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY event_seq, id) AS latest_rank
    FROM enriched
    QUALIFY latest_rank = 1
)
SELECT
    id,
    hire_date,
    job_title,
    last_known_job_title,
    last_known_dept_name,
    last_known_job_title_2,
    latest_rank,
    CASE WHEN job_title RLIKE '^(Manager|Director)$' THEN 'OTHER_ZONE' WHEN job_title RLIKE '^(Engineer|Analyst)$' THEN 'CORE' ELSE 'GROUP_B' END AS job_title_zone
FROM latest
ORDER BY hire_date DESC NULLS LAST;
