WITH metrics AS (
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
        AVG(salary) OVER (PARTITION BY dept_id ORDER BY salary, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) salary_avg_6,
        SUM(salary) OVER w_hist running_salary,
        PERCENT_RANK() OVER (PARTITION BY level ORDER BY salary) salary_percent_rank,
        SUM(salary) OVER (ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_salary,
        ROW_NUMBER() OVER w_hist_2 row_seq
    FROM employees
    WINDOW w_hist AS (PARTITION BY dept_id, level ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist_2 AS (PARTITION BY manager_id ORDER BY salary DESC, id)
),
enriched AS (
    -- Attach employees attributes
    SELECT s.*, b.level AS employee_level
    FROM metrics s
    LEFT JOIN employees b
        ON s.manager_id = b.id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT dept_id, job_title
    FROM enriched
    EXCEPT
    SELECT dept_id, job_title
    FROM employees
    WHERE job_title IN ('Manager', 'Analyst', 'Director')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN remaining_keys k ON s.dept_id = k.dept_id AND s.job_title = k.job_title
),
enriched_employees AS (
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
        salary_avg_6,
        running_salary,
        salary_percent_rank,
        cumulative_salary,
        row_seq,
        employee_level,
        MIN(running_salary) OVER (PARTITION BY manager_id) manager_id_min_running_salary,
        CUME_DIST() OVER (ORDER BY cumulative_salary DESC) cumulative_salary_cume_dist,
        RANK() OVER w_running cumulative_salary_rank
    FROM verified
    WINDOW w_running AS (PARTITION BY level, dept_id ORDER BY cumulative_salary DESC NULLS FIRST)
)
SELECT
    s.id,
    s.full_name,
    s.email,
    s.dept_id,
    s.manager_id,
    s.salary,
    s.hire_date,
    s.is_remote,
    s.level,
    (SELECT SUM(x.salary) FROM employees x WHERE x.manager_id = s.manager_id) AS sum_employee_salary
FROM enriched_employees s;
