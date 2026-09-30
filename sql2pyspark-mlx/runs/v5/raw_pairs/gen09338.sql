WITH joined AS (
    -- Attach employees attributes
    SELECT
        a.id,
        a.email,
        a.dept_id,
        a.manager_id,
        a.level,
        p.salary AS employee_salary,
        p.hire_date AS employee_hire_date
    FROM employees a
    LEFT JOIN employees p
        ON a.manager_id <=> p.id
    WHERE p.level != 'L5'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY employee_hire_date DESC NULLS FIRST, id ASC) AS rank_in_group
    FROM joined
    QUALIFY rank_in_group = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_id,
        employee_salary,
        employee_hire_date,
        rank_in_group,
        SUM(employee_salary) OVER w_hist AS cumulative_employee_salary,
        ROUND(employee_salary / SUM(employee_salary) OVER (PARTITION BY manager_id, dept_id), 4) AS employee_salary_share,
        CUME_DIST() OVER (PARTITION BY manager_id ORDER BY employee_salary ASC NULLS FIRST) AS employee_salary_cume_dist,
        DENSE_RANK() OVER (ORDER BY employee_salary DESC NULLS LAST) AS employee_salary_dense_rank,
        LEAD(employee_salary, 1, 0) OVER (PARTITION BY manager_id ORDER BY employee_salary ASC, id ASC) AS next_employee_salary
    FROM ranked
    WINDOW w_hist AS (PARTITION BY level, manager_id ORDER BY employee_salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY RANK() OVER (PARTITION BY dept_id ORDER BY next_employee_salary DESC) = 1
)
-- Window metrics over several different partitions
SELECT
    id,
    dept_id,
    employee_salary,
    employee_hire_date,
    rank_in_group,
    cumulative_employee_salary,
    employee_salary_share,
    employee_salary_cume_dist,
    FIRST_VALUE(employee_salary_cume_dist, TRUE) OVER (PARTITION BY dept_id ORDER BY employee_salary_dense_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_employee_salary_cume_dist,
    DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY employee_salary_cume_dist DESC NULLS LAST) AS employee_salary_cume_dist_dense_rank,
    CUME_DIST() OVER (PARTITION BY dept_id ORDER BY employee_salary ASC NULLS FIRST) AS employee_salary_cume_dist_2
FROM latest;
