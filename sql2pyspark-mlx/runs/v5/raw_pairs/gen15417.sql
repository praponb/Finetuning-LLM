WITH with_dims AS (
    -- Attach employees attributes
    SELECT
        e.id,
        e.full_name,
        e.manager_id,
        e.job_title,
        e.salary,
        e.hire_date,
        e.is_remote,
        p2.email AS employee_email,
        p2.job_title AS employee_job_title
    FROM employees e
    INNER JOIN employees p2
        ON e.manager_id = p2.id
    WHERE p2.level = 'L5'
),
kept_keys AS (
    -- Set difference on keys
    SELECT job_title, manager_id
    FROM with_dims
    EXCEPT ALL
    SELECT job_title, manager_id
    FROM employees
    WHERE level = 'L1'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN kept_keys k ON s.job_title = k.job_title AND s.manager_id = k.manager_id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY salary DESC) AS salary_cume_dist,
        AVG(salary) OVER w_grp AS job_title_avg_salary,
        CUME_DIST() OVER (PARTITION BY manager_id, job_title ORDER BY salary DESC) AS salary_cume_dist_2,
        MIN(salary) OVER w_grp AS job_title_min_salary,
        AVG(salary) OVER w_part AS manager_id_avg_salary
    FROM kept_rows
    WINDOW w_grp AS (PARTITION BY job_title), w_part AS (PARTITION BY manager_id)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        manager_id,
        job_title,
        salary,
        hire_date,
        employee_email,
        employee_job_title,
        job_title_avg_salary,
        CUME_DIST() OVER (PARTITION BY job_title ORDER BY salary_cume_dist DESC NULLS FIRST) AS salary_cume_dist_cume_dist,
        ROUND(manager_id_avg_salary / SUM(manager_id_avg_salary) OVER (PARTITION BY manager_id), 4) AS manager_id_avg_salary_share,
        ROUND(salary / SUM(salary) OVER (PARTITION BY employee_job_title), 4) AS salary_share,
        COUNT(manager_id_avg_salary) OVER (PARTITION BY job_title) AS job_title_count_manager_id_avg_salary,
        MIN(manager_id_avg_salary) OVER (PARTITION BY job_title) AS job_title_min_manager_id_avg_salary
    FROM metrics
)
SELECT
    id,
    employee_job_title,
    job_title_avg_salary,
    salary_cume_dist_cume_dist,
    salary_share,
    job_title_count_manager_id_avg_salary,
    job_title_min_manager_id_avg_salary,
    CASE WHEN employee_job_title RLIKE '^(Designer|Engineer)$' THEN 'EXTENDED' WHEN employee_job_title RLIKE '^(Analyst|Manager)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS employee_job_title_zone
FROM with_windows;
