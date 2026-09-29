WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        TRIM(job_title) AS clean_job_title
    FROM employees
    WHERE hire_date >= DATE '2023-09-09'
      AND YEAR(hire_date) = 2023
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        dept_id,
        manager_id,
        salary,
        is_remote,
        level
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date ASC NULLS LAST, id ASC) AS rank_in_group
        FROM valid
    ) ranked
    WHERE rank_in_group = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(level) IGNORE NULLS OVER (PARTITION BY level ORDER BY salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level,
        NTILE(5) OVER (PARTITION BY level ORDER BY salary DESC, id ASC) AS salary_bucket,
        MAX(salary) OVER (PARTITION BY dept_id, manager_id) AS dept_id_max_salary
    FROM ranked
),
kept_keys AS (
    -- Set difference on keys
    SELECT dept_id, manager_id
    FROM with_windows
    EXCEPT ALL
    SELECT dept_id, manager_id
    FROM employees
    WHERE dept_id = 34
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.dept_id = k.dept_id AND s.manager_id = k.manager_id
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, dept_id
    FROM verified
    INTERSECT
    SELECT id, dept_id
    FROM employees
    WHERE is_remote = TRUE
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.dept_id = k.dept_id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        dept_id,
        manager_id,
        salary,
        is_remote,
        first_known_level,
        salary_bucket,
        dept_id_max_salary,
        CASE WHEN level IN ('L4', 'L5') THEN 'Core' ELSE 'Other' END AS level_group,
        COALESCE(level, 'DIRECT') AS level_filled
    FROM reconciled
    WHERE level IS DISTINCT FROM 'L2'
)
SELECT
    s.id,
    s.manager_id,
    s.salary,
    s.first_known_level,
    s.dept_id_max_salary,
    s.level_group,
    (SELECT MIN(e2.salary) FROM employees e2 WHERE e2.manager_id = s.manager_id) AS min_employee_salary
FROM base s
ORDER BY min_employee_salary DESC NULLS LAST, id ASC;
