WITH cleaned AS (
    -- Basic cleansing and defaults
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
        LENGTH(full_name) AS full_name_length,
        CASE WHEN is_remote THEN 'Yes' ELSE 'No' END AS is_remote_label,
        COALESCE(level, 'OTHER') AS level_clean
    FROM employees
    WHERE level IS NOT NULL
      AND manager_id IS NULL
),
enriched AS (
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
        full_name_length,
        is_remote_label,
        level_clean,
        RANK() OVER (PARTITION BY job_title ORDER BY salary DESC) AS salary_rank,
        DENSE_RANK() OVER (PARTITION BY manager_id ORDER BY salary DESC) AS salary_dense_rank
    FROM cleaned
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        a.*,
        d.budget,
        d.dept_name
    FROM enriched a
    LEFT JOIN departments d
        ON a.dept_id = d.id
),
with_dims_employees AS (
    -- Attach departments attributes
    SELECT src.*, p.dept_name AS department_dept_name
    FROM with_dims src
    LEFT JOIN departments p
        ON src.dept_id = p.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(salary_dense_rank, 1, 0) OVER (PARTITION BY dept_name, dept_id ORDER BY hire_date, id) AS prev_salary_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary, id) AS row_seq
    FROM with_dims_employees
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        full_name_length,
        is_remote_label,
        salary_rank,
        salary_dense_rank,
        budget,
        department_dept_name,
        LOWER(department_dept_name) AS department_dept_name_lower
    FROM with_windows
    WHERE manager_id IS NOT NULL
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT dept_id, id
    FROM valid
    INTERSECT ALL
    SELECT dept_id, id
    FROM employees
    WHERE salary > 73000
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN remaining_keys k ON s.dept_id = k.dept_id AND s.id = k.id
)
-- Ranking and gap features
SELECT
    id,
    full_name,
    email,
    dept_id,
    job_title,
    full_name_length,
    is_remote_label,
    salary_rank,
    salary_dense_rank,
    budget,
    department_dept_name,
    department_dept_name_lower,
    DENSE_RANK() OVER w_recent AS budget_dense_rank,
    RANK() OVER w_running AS salary_dense_rank_rank,
    AVG(full_name_length) OVER w_recent_2 AS department_dept_name_avg_full_name_length
FROM reconciled
WINDOW w_recent AS (PARTITION BY department_dept_name ORDER BY budget DESC NULLS LAST), w_running AS (PARTITION BY job_title ORDER BY salary_dense_rank DESC NULLS LAST), w_recent_2 AS (PARTITION BY department_dept_name)
