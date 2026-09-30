WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT location
    FROM departments
    INTERSECT
    SELECT location
    FROM departments
    WHERE budget IS NOT NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM departments s
    INNER JOIN remaining_keys k ON s.location = k.location
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY budget DESC NULLS LAST) AS budget_percent_rank,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) AS budget_share
    FROM reconciled
    WHERE location IN ('London', 'New York', 'Bangkok')
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN location = 'Singapore' THEN 1 ELSE 0 END AS is_singapore,
        LOWER(dept_name) AS dept_name_lower,
        COALESCE(dept_name, 'N/A') AS dept_name_filled
    FROM metrics
    WHERE dept_name IS DISTINCT FROM 'Marketing'
),
matched_keys AS (
    -- Set intersection on keys
    SELECT location, id
    FROM filtered
    INTERSECT
    SELECT location, id
    FROM departments
    WHERE dept_name IN ('Legal', 'Sales')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.location = k.location AND s.id = k.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.salary BETWEEN 78000 AND 128000
        )
)
SELECT
    id,
    dept_name,
    location,
    budget_percent_rank,
    budget_share,
    dept_name_lower,
    dept_name_filled
FROM not_flagged
