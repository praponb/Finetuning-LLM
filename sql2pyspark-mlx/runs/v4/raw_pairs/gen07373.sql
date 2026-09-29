WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CEIL(salary) salary_whole,
        COALESCE(job_title, 'N/A') resolved_job_title
    FROM employees
    WHERE dept_id = 31
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date DESC, id) = 1
),
enriched AS (
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
        salary_whole,
        resolved_job_title,
        NTILE(3) OVER (ORDER BY salary DESC, id) salary_bucket,
        MAX(salary_whole) OVER (PARTITION BY dept_id, job_title) dept_id_max_salary_whole,
        SUM(salary_whole) OVER (PARTITION BY job_title ORDER BY salary_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_salary_whole,
        CUME_DIST() OVER (PARTITION BY dept_id ORDER BY salary_whole NULLS LAST) salary_whole_cume_dist
    FROM deduped
),
metrics AS (
    -- Window metrics over several different partitions
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
        salary_whole,
        resolved_job_title,
        salary_bucket,
        dept_id_max_salary_whole,
        running_salary_whole,
        salary_whole_cume_dist,
        AVG(dept_id_max_salary_whole) OVER (PARTITION BY dept_id ORDER BY salary, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) dept_id_max_salary_whole_avg_6,
        AVG(running_salary_whole) OVER (PARTITION BY level ORDER BY running_salary_whole, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) running_salary_whole_avg_2_prior
    FROM enriched
),
enriched_employees AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(dept_id_max_salary_whole_avg_6, 2) OVER (PARTITION BY manager_id, job_title ORDER BY dept_id_max_salary_whole_avg_6, id) next_dept_id_max_salary_whole_avg_6,
        SUM(salary_bucket) OVER (PARTITION BY dept_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_salary_bucket,
        LAST_VALUE(salary_whole) IGNORE NULLS OVER w_running last_known_salary_whole,
        ROW_NUMBER() OVER (ORDER BY salary_whole, id) event_seq
    FROM metrics
    WINDOW w_running AS (PARTITION BY job_title, dept_id ORDER BY salary_whole_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        e.*,
        b.level AS employee_level,
        b.full_name AS employee_full_name,
        b.hire_date AS employee_hire_date
    FROM enriched_employees e
    JOIN employees b
        ON e.manager_id = b.id
)
SELECT
    id,
    full_name,
    job_title,
    hire_date,
    level,
    salary_whole,
    resolved_job_title,
    salary_bucket,
    running_salary_whole,
    CASE WHEN job_title RLIKE '^(Designer|Analyst)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS job_title_zone
FROM with_dims
ORDER BY level DESC NULLS LAST
