WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(salary, 1) salary_rounded,
        SPLIT(LOWER(email), '@')[0] email_user
    FROM employees
    WHERE hire_date >= DATE '2023-08-26'
      AND job_title IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT dept_id
    FROM base
    EXCEPT ALL
    SELECT dept_id
    FROM employees
    WHERE level IN ('L5', 'L4')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN remaining_keys k ON s.dept_id = k.dept_id
),
base_employees AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        salary + salary_rounded salary_plus_salary_rounded,
        COALESCE(level, 'N/A') resolved_level
    FROM verified
    WHERE hire_date >= DATE '2023-04-30'
      AND level IS NOT DISTINCT FROM 'L3'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT *, CUME_DIST() OVER (ORDER BY salary_plus_salary_rounded DESC NULLS FIRST) salary_plus_salary_rounded_cume_dist
    FROM base_employees
),
joined AS (
    -- Attach employees attributes
    SELECT
        s.id,
        s.full_name,
        s.manager_id,
        s.job_title,
        s.salary,
        s.is_remote,
        s.level,
        s.salary_rounded,
        s.email_user,
        s.salary_plus_salary_rounded,
        s.salary_plus_salary_rounded_cume_dist,
        b.full_name AS employee_full_name,
        b.salary AS employee_salary
    FROM scored s
    LEFT JOIN employees b
        ON s.manager_id = b.id
),
enriched AS (
    -- Attach employees attributes
    SELECT e.*, p.full_name AS employee_full_name_2
    FROM joined e
    LEFT JOIN employees p
        ON e.manager_id = p.id
),
with_dims AS (
    -- Attach employees attributes
    SELECT e.*, p2.level AS employee_level
    FROM enriched e
    INNER JOIN employees p2
        ON e.manager_id <=> p2.id
)
SELECT
    full_name,
    job_title,
    salary,
    level,
    email_user,
    salary_plus_salary_rounded,
    salary_plus_salary_rounded_cume_dist,
    employee_salary,
    employee_full_name_2,
    CASE WHEN job_title RLIKE '^(Manager|Director)$' THEN 'GROUP_B' WHEN job_title RLIKE '^(Engineer|Analyst)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS job_title_zone
FROM with_dims;
