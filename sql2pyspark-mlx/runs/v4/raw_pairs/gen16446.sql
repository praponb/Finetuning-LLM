WITH kept_keys AS (
    -- Set difference on keys
    SELECT id, job_title
    FROM employees
    EXCEPT
    SELECT id, job_title
    FROM employees
    WHERE salary > 121000
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM employees s
    INNER JOIN kept_keys k ON s.id = k.id AND s.job_title = k.job_title
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        ROUND(salary / SUM(salary) OVER (PARTITION BY job_title), 4) AS salary_share,
        ROUND(salary / SUM(salary) OVER (PARTITION BY dept_id), 4) AS salary_share_2,
        ROUND(salary / SUM(salary) OVER (PARTITION BY job_title), 4) AS salary_share_3
    FROM kept_rows
),
matched_keys AS (
    -- Set difference on keys
    SELECT job_title, manager_id
    FROM scored
    EXCEPT
    SELECT job_title, manager_id
    FROM employees
    WHERE hire_date <= '2025-08-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.job_title = k.job_title AND s.manager_id = k.manager_id
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        s.*,
        p.hire_date AS employee_hire_date,
        p.salary AS employee_salary
    FROM verified s
    LEFT JOIN employees p
        ON s.manager_id = p.id
),
totals AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('YEAR', employee_hire_date) AS year_start,
        CASE WHEN salary_share_3 >= 0.69 THEN 'HIGH' ELSE 'LOW' END AS salary_share_3_tier,
        COUNT(DISTINCT job_title) AS distinct_job_titles,
        SIZE(COLLECT_SET(job_title)) AS num_job_titles,
        PERCENTILE_APPROX(salary_share, ARRAY(0.25, 0.5, 0.75)) AS salary_share_quartiles,
        COUNT_IF(job_title = 'Designer') AS designer_events,
        AVG(salary) AS avg_salary,
        APPROX_PERCENTILE(salary, 0.95) AS salary_p95,
        GROUPING_ID() AS gid
    FROM with_dims
    GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', employee_hire_date), CASE WHEN salary_share_3 >= 0.69 THEN 'HIGH' ELSE 'LOW' END), (DATE_TRUNC('YEAR', employee_hire_date)))
)
SELECT
    year_start,
    salary_share_3_tier,
    distinct_job_titles,
    num_job_titles,
    salary_share_quartiles,
    avg_salary,
    salary_p95,
    gid
FROM totals
ORDER BY year_start DESC;
