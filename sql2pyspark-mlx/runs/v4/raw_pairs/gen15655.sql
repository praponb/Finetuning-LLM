WITH remaining_keys AS (
    -- Set difference on keys
    SELECT manager_id
    FROM employees
    EXCEPT
    SELECT manager_id
    FROM employees
    WHERE salary < 112000
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM employees s
    INNER JOIN remaining_keys k ON s.manager_id = k.manager_id
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        s.*,
        b.job_title AS employee_job_title,
        b.hire_date AS employee_hire_date,
        b.salary AS employee_salary
    FROM kept_rows s
    LEFT JOIN employees b
        ON s.manager_id = b.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY manager_id ORDER BY employee_salary DESC NULLS FIRST) employee_salary_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY job_title, dept_id ORDER BY salary) salary_percent_rank,
        CUME_DIST() OVER (ORDER BY employee_salary DESC NULLS FIRST) employee_salary_cume_dist,
        SUM(salary) OVER (PARTITION BY job_title) job_title_sum_salary
    FROM with_dims
),
enriched_employees AS (
    -- Running and moving aggregates
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
        employee_job_title,
        employee_hire_date,
        employee_salary,
        employee_salary_percent_rank,
        salary_percent_rank,
        employee_salary_cume_dist,
        job_title_sum_salary,
        DENSE_RANK() OVER (PARTITION BY manager_id, dept_id ORDER BY employee_salary_cume_dist DESC NULLS FIRST) employee_salary_cume_dist_dense_rank,
        MAX(job_title_sum_salary) OVER (PARTITION BY dept_id) dept_id_max_job_title_sum_salary,
        PERCENT_RANK() OVER (PARTITION BY employee_job_title ORDER BY salary_percent_rank DESC) salary_percent_rank_percent_rank,
        ROUND(employee_salary / SUM(employee_salary) OVER (PARTITION BY job_title, level), 4) employee_salary_share
    FROM enriched
),
joined AS (
    -- Attach employees attributes
    SELECT
        s.*,
        p.email AS employee_email,
        p.level AS employee_level
    FROM enriched_employees s
    LEFT JOIN employees p
        ON s.manager_id = p.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY employee_salary_percent_rank DESC NULLS LAST) employee_salary_percent_rank_cume_dist,
        RANK() OVER (ORDER BY employee_salary DESC NULLS LAST) employee_salary_rank
    FROM joined
)
-- Running and moving aggregates
SELECT
    *,
    PERCENT_RANK() OVER (PARTITION BY employee_job_title ORDER BY employee_salary_percent_rank_cume_dist) employee_salary_percent_rank_cume_dist_percent_rank,
    CUME_DIST() OVER (ORDER BY employee_salary_cume_dist_dense_rank) employee_salary_cume_dist_dense_rank_cume_dist,
    PERCENT_RANK() OVER (ORDER BY employee_salary_share) employee_salary_share_percent_rank,
    PERCENT_RANK() OVER (ORDER BY employee_salary_percent_rank NULLS LAST) employee_salary_percent_rank_percent_rank,
    PERCENT_RANK() OVER (ORDER BY employee_salary) employee_salary_percent_rank_2
FROM with_windows;
