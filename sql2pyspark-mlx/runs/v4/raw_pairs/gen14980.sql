WITH base AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(dept_name, ', ', location) AS dept_name_location
    FROM departments
    WHERE location IS NOT NULL
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.hire_date,
        top1.employee_salary
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.hire_date, c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY employee_salary DESC NULLS FIRST, id) AS latest_rank
    FROM with_last
    QUALIFY latest_rank = 1
),
deduped_departments AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY employee_salary DESC, id) AS rank_in_group
    FROM deduped
    QUALIFY rank_in_group = 1
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        dept_name,
        location,
        employee_salary
    FROM deduped_departments
) src
PIVOT (
    MAX(employee_salary) AS total,
    COUNT(1) AS cnt
    FOR location IN ('Chiang Mai' AS chiang_mai, 'Tokyo' AS tokyo, 'New York' AS new_york)
);
