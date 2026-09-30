WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, DATEDIFF(CURRENT_DATE(), hire_date) AS days_since_hire_date
    FROM employees
    WHERE hire_date > DATE '2023-02-21'
      AND manager_id IS NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        is_remote,
        days_since_hire_date,
        ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY days_since_hire_date, id) AS row_seq,
        SUM(days_since_hire_date) OVER (PARTITION BY job_title ORDER BY days_since_hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_hire_date
    FROM cleaned
    WHERE LOWER(TRIM(full_name)) = 'mia rossi'
),
enriched AS (
    -- Attach departments attributes
    SELECT
        s.id,
        s.full_name,
        s.email,
        s.dept_id,
        s.job_title,
        s.row_seq,
        d.dept_name,
        d.budget
    FROM metrics s
    LEFT JOIN departments d
        ON s.dept_id = d.id
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        email,
        dept_id,
        dept_name,
        budget
    FROM enriched
    QUALIFY RANK() OVER (PARTITION BY dept_id ORDER BY budget DESC NULLS FIRST) = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        email,
        dept_id,
        dept_name,
        budget,
        CEIL(budget) AS budget_whole,
        COALESCE(dept_name, 'NONE') AS resolved_dept_name
    FROM deduped
    WHERE dept_name IS NOT NULL
)
SELECT
    id,
    dept_id,
    dept_name,
    budget,
    budget_whole,
    resolved_dept_name
FROM filtered
ORDER BY dept_name DESC;
