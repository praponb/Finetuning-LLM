WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(dept_name) AS dept_name_upper,
        INITCAP(TRIM(location)) AS location_title
    FROM departments
    WHERE dept_name IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS rn
        FROM cleaned
    ) x
    WHERE rn = 1
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        dept_name,
        location,
        budget,
        dept_name_upper,
        location_title,
        rn,
        budget + rn AS budget_plus_rn,
        budget * rn AS budget_times_rn
    FROM deduped
    WHERE location <> 'Bangkok'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.salary BETWEEN 74000 AND 91000
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND NOT ch.is_remote
        )
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.location,
        s.location_title,
        s.budget_plus_rn,
        s.budget_times_rn,
        latest.employee_level,
        latest.salary
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.level AS employee_level, c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, salary - budget_plus_rn AS salary_minus_budget_plus_rn
    FROM with_last
    WHERE location IN ('Chiang Mai', 'Singapore', 'Tokyo')
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            dept_name,
            location,
            employee_level,
            budget_times_rn
        FROM valid
    ) src
    PIVOT (
        AVG(budget_times_rn) AS value_sum,
        COUNT(1) AS n
        FOR employee_level IN ('L1' AS l1, 'L2' AS l2)
    )
)
SELECT
    dept_name,
    location,
    l1_value_sum,
    l1_n,
    CASE WHEN location RLIKE '^(Berlin|Singapore)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS location_zone
FROM wide
ORDER BY location NULLS FIRST
