WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        salary,
        is_remote,
        level,
        DATE_TRUNC('MONTH', hire_date) AS hire_date_period,
        COALESCE(level, 'OTHER') AS resolved_level
    FROM employees
    WHERE manager_id IS NOT NULL
      AND job_title IS DISTINCT FROM 'Manager'
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(hire_date_period) OVER (PARTITION BY dept_id ORDER BY hire_date_period, id) AS prev_hire_date_period
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_hire_date_period IS NULL OR hire_date_period > prev_hire_date_period + INTERVAL '60' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        dept_id,
        salary,
        hire_date_period,
        SUM(new_group_flag) OVER (PARTITION BY dept_id ORDER BY hire_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        dept_id,
        salary
    FROM islands
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC, id) = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(salary, 0) AS salary_rounded,
        CAST(salary AS INT) AS salary_int
    FROM ranked
    WHERE salary < 105000
),
enriched AS (
    -- Attach departments attributes
    SELECT a.*, d.dept_name
    FROM base a
    LEFT JOIN departments d
        ON a.dept_id = d.id
),
joined AS (
    -- Attach departments attributes
    SELECT
        s.id,
        s.dept_id,
        s.salary,
        s.salary_int,
        s.dept_name,
        p.budget,
        p.dept_name AS department_dept_name
    FROM enriched s
    LEFT JOIN departments p
        ON s.dept_id = p.id
),
joined_employees AS (
    -- Attach departments attributes
    SELECT
        src.id,
        src.dept_id,
        src.dept_name,
        src.department_dept_name,
        b.location,
        b.dept_name AS department_dept_name_2,
        b.budget AS department_budget
    FROM joined src
    INNER JOIN departments b
        ON src.dept_id = b.id
)
SELECT
    id,
    dept_name,
    department_dept_name,
    location,
    department_budget
FROM joined_employees
ORDER BY department_budget DESC NULLS LAST, department_dept_name
