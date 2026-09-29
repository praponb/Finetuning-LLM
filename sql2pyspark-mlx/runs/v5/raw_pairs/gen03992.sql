WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        email,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        CASE WHEN salary >= 123000 THEN 'High' WHEN salary >= 86000 THEN 'Medium' ELSE 'Low' END AS salary_level,
        ADD_MONTHS(hire_date, 1) AS hire_date_plus_months,
        COALESCE(level, 'NONE') AS resolved_level
    FROM employees
    WHERE hire_date > TO_DATE('2023-07-12')
      AND level IN ('L5', 'L4', 'L2')
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        src.id,
        src.manager_id,
        src.salary,
        src.level,
        src.resolved_level,
        d.email AS employee_email,
        d.salary AS employee_salary
    FROM valid src
    LEFT JOIN employees d
        ON src.manager_id = d.id
    WHERE d.job_title IN ('Intern', 'Manager', 'Director')
),
with_dims_employees AS (
    -- Attach employees attributes
    SELECT
        src.*,
        d.email,
        d.salary AS employee_salary_2,
        d.hire_date
    FROM with_dims src
    INNER JOIN employees d
        ON src.manager_id = d.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        salary,
        employee_email,
        employee_salary,
        email,
        LOWER(employee_email) AS employee_email_lower,
        COALESCE(level, 'N/A') AS level_clean
    FROM with_dims_employees
    WHERE hire_date > DATE '2023-07-26'
      AND hire_date < '2023-09-01'
),
base AS (
    -- Keep recent, valid rows
    SELECT *, employee_salary * salary AS employee_salary_times_salary
    FROM filtered
    WHERE salary <= 93000
)
SELECT
    id,
    salary,
    employee_email,
    email,
    employee_email_lower,
    level_clean,
    employee_salary_times_salary
FROM base
