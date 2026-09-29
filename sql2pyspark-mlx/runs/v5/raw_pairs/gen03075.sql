WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN dept_name IN ('Marketing', 'Engineering') THEN 'Priority' ELSE 'Other' END AS dept_name_group,
        ROUND(budget * 0.9, 2) AS budget_adjusted,
        COALESCE(dept_name, 'N/A') AS dept_name_clean
    FROM departments
    WHERE location IS NOT NULL
      AND location IS DISTINCT FROM 'Tokyo'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        dept_name,
        location,
        dept_name_group,
        budget_adjusted,
        dept_name_clean,
        TRIM(location) AS clean_location,
        COALESCE(location, 'UNKNOWN') AS location_clean
    FROM cleaned
    WHERE location IS NOT NULL
      AND location = 'New York'
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.location,
        s.dept_name_group,
        s.location_clean,
        latest.salary,
        latest.employee_hire_date
    FROM base s
    JOIN LATERAL (
            SELECT c.salary, c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY employee_hire_date DESC NULLS LAST, id) AS latest_rank
        FROM with_last
    ) t
    WHERE latest_rank = 1
)
SELECT
    id,
    dept_name_group,
    location_clean,
    salary,
    employee_hire_date,
    latest_rank
FROM deduped;
