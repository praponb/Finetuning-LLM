WITH kept_keys AS (
    -- Set difference on keys
    SELECT manager_id, id
    FROM employees
    EXCEPT
    SELECT manager_id, id
    FROM employees
    WHERE NOT is_remote
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM employees s
    INNER JOIN kept_keys k ON s.manager_id = k.manager_id AND s.id = k.id
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        a.id,
        a.email,
        a.dept_id,
        a.manager_id,
        a.salary,
        a.hire_date,
        a.is_remote,
        a.level,
        d.email AS employee_email
    FROM reconciled a
    INNER JOIN employees d
        ON a.manager_id <=> d.id
    WHERE LOWER(TRIM(d.email)) = 'ivy@gmail.com'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(salary) OVER (PARTITION BY dept_id) AS dept_id_max_salary,
        DENSE_RANK() OVER (ORDER BY salary DESC) AS salary_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY manager_id, dept_id ORDER BY salary DESC) AS salary_percent_rank,
        ROUND(salary / SUM(salary) OVER (PARTITION BY level), 4) AS salary_share,
        DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY salary) AS salary_dense_rank_2
    FROM with_dims
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT level, manager_id
    FROM enriched
    INTERSECT ALL
    SELECT level, manager_id
    FROM employees
    WHERE YEAR(hire_date) >= 2023
),
reconciled_employees AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.level = k.level AND s.manager_id = k.manager_id
),
remaining_keys_2 AS (
    -- Set difference on keys
    SELECT dept_id, id
    FROM reconciled_employees
    EXCEPT DISTINCT
    SELECT dept_id, id
    FROM employees
    WHERE dept_id IN (3, 23, 31, 37)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled_employees s
    INNER JOIN remaining_keys_2 k ON s.dept_id = k.dept_id AND s.id = k.id
)
SELECT
    id,
    email,
    manager_id,
    salary,
    hire_date,
    employee_email,
    dept_id_max_salary,
    salary_dense_rank,
    salary_percent_rank
FROM verified
ORDER BY salary_percent_rank, salary_dense_rank
