WITH with_dims AS (
    -- Attach employees attributes
    SELECT
        e.*,
        p.email AS employee_email,
        p.level AS employee_level,
        p.hire_date AS employee_hire_date
    FROM employees e
    LEFT JOIN employees p
        ON e.manager_id = p.id
),
enriched AS (
    -- Attach employees attributes
    SELECT
        src.*,
        p.job_title AS employee_job_title,
        p.full_name AS employee_full_name,
        p.salary AS employee_salary
    FROM with_dims src
    LEFT JOIN employees p
        ON src.manager_id = p.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY employee_level, job_title ORDER BY employee_salary DESC NULLS FIRST, id) AS row_num
        FROM enriched
    ) t
    WHERE row_num = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(salary) OVER w_grp AS cumulative_salary,
        LEAD(salary, 1, 0) OVER (ORDER BY employee_salary, id) AS next_salary
    FROM latest
    WINDOW w_grp AS (PARTITION BY employee_level, job_title ORDER BY employee_hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    -- Set difference on keys
    SELECT level
    FROM scored
    EXCEPT ALL
    SELECT level
    FROM employees
    WHERE level = 'L5'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.level = k.level
),
enriched_employees AS (
    -- Attach employees attributes
    SELECT
        src.*,
        p.full_name AS employee_full_name_2,
        p.email AS employee_email_2
    FROM verified src
    INNER JOIN employees p
        ON src.manager_id = p.id
),
enriched_employees_2 AS (
    -- Attach departments attributes
    SELECT
        e.*,
        d.location,
        d.budget,
        d.dept_name
    FROM enriched_employees e
    INNER JOIN departments d
        ON e.dept_id = d.id
)
SELECT
    s.email,
    s.manager_id,
    s.job_title,
    s.salary,
    s.hire_date,
    s.is_remote,
    s.level,
    s.employee_email,
    s.employee_level,
    CASE WHEN employee_level RLIKE '^(L3|L2)$' THEN 'EXTENDED' WHEN employee_level RLIKE '^(L1|L4)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS employee_level_zone,
    (SELECT AVG(t2.salary) FROM employees t2 WHERE t2.manager_id = s.manager_id) AS avg_employee_salary
FROM enriched_employees_2 s
ORDER BY level DESC, avg_employee_salary DESC;
