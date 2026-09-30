WITH metrics AS (
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
        NTILE(4) OVER (PARTITION BY level, dept_id ORDER BY salary, id) AS salary_bucket,
        LEAD(salary, 1) OVER w_grp AS next_salary,
        MIN(salary) OVER (PARTITION BY manager_id) AS manager_id_min_salary
    FROM employees
    WINDOW w_grp AS (PARTITION BY level ORDER BY salary, id)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date DESC NULLS LAST, id) AS rn
        FROM metrics
    ) t
    WHERE rn = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT dept_id, job_title
    FROM ranked
    EXCEPT
    SELECT dept_id, job_title
    FROM employees
    WHERE level IN ('L5', 'L4')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN remaining_keys k ON s.dept_id = k.dept_id AND s.job_title = k.job_title
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        e.*,
        b.job_title AS employee_job_title,
        b.level AS employee_level
    FROM kept_rows e
    INNER JOIN employees b
        ON e.manager_id = b.id
),
enriched AS (
    -- Attach departments attributes
    SELECT
        e.*,
        p.location,
        p.dept_name,
        p.budget
    FROM with_dims e
    LEFT JOIN departments p
        ON e.dept_id = p.id
)
SELECT
    s.id,
    s.full_name,
    s.dept_id,
    s.manager_id,
    s.job_title,
    s.is_remote,
    s.level,
    s.next_salary,
    s.manager_id_min_salary,
    (SELECT MIN(t2.salary) FROM employees t2 WHERE t2.dept_id = s.dept_id) AS min_employee_salary
FROM enriched s
ORDER BY job_title DESC NULLS LAST, manager_id
