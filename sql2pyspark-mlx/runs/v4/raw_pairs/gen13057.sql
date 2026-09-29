WITH enriched AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget DESC, id) AS event_seq,
        MAX(budget) OVER (PARTITION BY location) AS location_max_budget,
        NTILE(10) OVER (PARTITION BY dept_name, location ORDER BY budget, id) AS budget_bucket
    FROM departments
),
cleaned AS (
    SELECT *, CONCAT(location, '/', dept_name) AS location_dept_name
    FROM enriched
    WHERE location IN ('New York', 'Bangkok')
),
eligible AS (
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.hire_date <= '2025-08-01'
        )
),
cleaned_departments AS (
    SELECT
        *,
        CASE WHEN location_max_budget >= 3356247 THEN 'High' WHEN location_max_budget >= 1483713 THEN 'Medium' ELSE 'Low' END AS location_max_budget_tier,
        COALESCE(location, 'DIRECT') AS resolved_location
    FROM eligible
    WHERE budget_bucket != 8
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY location_max_budget DESC, id) AS latest_rank
        FROM cleaned_departments
    ) x
    WHERE latest_rank = 1
)
SELECT
    id,
    dept_name,
    location,
    budget,
    location_max_budget,
    budget_bucket,
    location_max_budget_tier,
    resolved_location,
    latest_rank,
    CASE WHEN location RLIKE '^(Chiang Mai|Berlin)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS location_zone
FROM latest
ORDER BY location DESC;
