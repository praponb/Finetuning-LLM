WITH enriched AS (
    SELECT
        e.*,
        p.salary AS employee_salary,
        p.full_name AS employee_full_name,
        p.level AS employee_level
    FROM employees e
    INNER JOIN employees p
        ON e.manager_id = p.id
),
valid AS (
    SELECT *, UPPER(full_name) AS full_name_upper
    FROM enriched
    WHERE employee_level = 'L4'
),
joined AS (
    SELECT s.*, b.hire_date AS employee_hire_date
    FROM valid s
    INNER JOIN employees b
        ON s.manager_id = b.id
),
with_windows AS (
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
        employee_salary,
        employee_full_name,
        employee_level,
        full_name_upper,
        employee_hire_date,
        AVG(employee_salary) OVER (ORDER BY employee_salary, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS employee_salary_avg_3,
        ROW_NUMBER() OVER w_grp AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date DESC, id) AS event_seq_2
    FROM joined
    WINDOW w_grp AS (PARTITION BY job_title ORDER BY salary, id)
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY manager_id ORDER BY event_seq_2 DESC NULLS LAST) AS dedup_rank
    FROM with_windows
    QUALIFY dedup_rank = 1
),
enriched_employees AS (
    SELECT s.*, d.location
    FROM ranked s
    LEFT JOIN departments d
        ON s.dept_id = d.id
),
joined_employees AS (
    SELECT src.*, p.email AS employee_email
    FROM enriched_employees src
    LEFT JOIN employees p
        ON src.manager_id = p.id
),
pivoted AS (
    SELECT *
    FROM (
        SELECT
            dept_id,
            level,
            employee_level,
            event_seq
        FROM joined_employees
    ) src
    PIVOT (
        SUM(event_seq) AS total,
        COUNT(1) AS n
        FOR employee_level IN ('L5', 'L2', 'L1')
    )
)
SELECT
    dept_id,
    level,
    L5_n,
    L2_n,
    L1_total,
    L1_n
FROM pivoted
ORDER BY L1_n DESC, dept_id DESC
