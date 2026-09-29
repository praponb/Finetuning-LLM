WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(location)) AS location_title
    FROM departments
    WHERE dept_name IS NOT DISTINCT FROM 'Finance'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(budget) OVER (PARTITION BY location ORDER BY budget, id) AS prev_budget,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) AS event_seq,
        DENSE_RANK() OVER (PARTITION BY location, dept_name ORDER BY budget DESC NULLS LAST) AS budget_dense_rank
    FROM cleaned
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.salary BETWEEN 123000 AND 141000
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.manager_id IN (4, 24)
        )
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        dept_name,
        location,
        location_title,
        prev_budget,
        event_seq
    FROM not_flagged
    QUALIFY ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY prev_budget DESC, id) = 1
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(prev_budget / event_seq, 2) AS prev_budget_per_event_seq,
        CONCAT(dept_name, ' ', location) AS dept_name_location
    FROM deduped
    WHERE location NOT IN ('Singapore', 'Berlin', 'Bangkok')
)
SELECT
    id,
    location,
    prev_budget,
    event_seq,
    prev_budget_per_event_seq,
    dept_name_location,
    CASE WHEN location RLIKE '^(New York|Chiang Mai)$' THEN 'PRIORITY' ELSE 'CORE' END AS location_zone
FROM filtered
