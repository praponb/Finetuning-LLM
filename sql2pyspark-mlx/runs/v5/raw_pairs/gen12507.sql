WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        salary,
        is_remote,
        IF(is_remote, 'Yes', 'No') is_remote_label,
        COALESCE(job_title, 'OTHER') resolved_job_title
    FROM employees
    WHERE level IS DISTINCT FROM 'L5'
      AND hire_date > DATE '2023-04-28'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY salary DESC) salary_cume_dist,
        LEAD(salary, 2) OVER (PARTITION BY manager_id ORDER BY salary, id) next_salary
    FROM valid
    WHERE NOT is_remote
),
with_dims AS (
    -- Attach departments attributes
    SELECT a.*, b.budget
    FROM scored a
    INNER JOIN departments b
        ON a.dept_id = b.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(salary) OVER (PARTITION BY dept_id ORDER BY salary_cume_dist, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_avg_salary,
        MAX(salary) OVER (PARTITION BY dept_id ORDER BY salary_cume_dist, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_max_salary,
        SUM(salary) OVER (PARTITION BY dept_id ORDER BY salary_cume_dist, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_sum_salary
    FROM with_dims
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id, manager_id ORDER BY last7_max_salary NULLS LAST, id) AS rn
        FROM with_windows
    ) t
    WHERE rn = 1
),
enriched AS (
    -- Attach departments attributes
    SELECT
        a.id,
        a.full_name,
        a.dept_id,
        a.manager_id,
        a.salary,
        a.salary_cume_dist,
        a.next_salary,
        a.budget,
        a.last7_avg_salary,
        a.last7_max_salary,
        a.rn,
        d.budget AS department_budget,
        d.location,
        d.dept_name
    FROM deduped a
    INNER JOIN departments d
        ON a.dept_id = d.id
)
SELECT
    id,
    dept_id,
    manager_id,
    salary,
    salary_cume_dist,
    next_salary,
    budget,
    last7_avg_salary,
    rn
FROM enriched
ORDER BY id DESC NULLS FIRST, rn DESC
