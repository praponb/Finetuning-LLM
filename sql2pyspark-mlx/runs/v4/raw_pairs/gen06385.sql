WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        LOWER(job_title) AS job_title_lower
    FROM employees
    WHERE job_title IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        job_title_lower,
        CUME_DIST() OVER (PARTITION BY job_title ORDER BY salary DESC) AS salary_cume_dist,
        ROW_NUMBER() OVER w_hist AS seq_no
    FROM valid
    WINDOW w_hist AS (PARTITION BY job_title ORDER BY salary, id)
),
matched_keys AS (
    -- Set difference on keys
    SELECT manager_id
    FROM with_windows
    EXCEPT
    SELECT manager_id
    FROM employees
    WHERE level IN ('L3', 'L5', 'L4')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    JOIN matched_keys k ON s.manager_id = k.manager_id
)
-- Grouped metrics
SELECT
    CASE WHEN salary >= 157000 THEN 'LARGE' ELSE 'SMALL' END AS salary_tier,
    APPROX_PERCENTILE(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles,
    AVG(salary_cume_dist) AS avg_salary_cume_dist,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles_2,
    COUNT(CASE WHEN job_title = 'Intern' THEN 1 END) AS intern_count,
    COUNT(*) AS event_count
FROM verified
GROUP BY salary_tier
ORDER BY salary_tier DESC NULLS LAST;
