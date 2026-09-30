WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(location, 1, 4) location_prefix,
        LOWER(dept_name) dept_name_lower,
        COALESCE(dept_name, 'N/A') resolved_dept_name
    FROM departments
    WHERE location IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        location,
        budget,
        location_prefix
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget NULLS FIRST, id) = 1
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND YEAR(x.hire_date) = 2023
        )
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS rn
        FROM not_flagged
    ) ranked
    WHERE rn = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(budget) OVER w_ordered last3_count_budget,
        MAX(budget) OVER w_ordered last3_max_budget,
        MIN(budget) OVER w_ordered last3_min_budget,
        PERCENT_RANK() OVER w_hist budget_percent_rank
    FROM ranked
    WINDOW w_ordered AS (PARTITION BY location ORDER BY rn, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY location ORDER BY budget DESC NULLS LAST)
)
SELECT
    id,
    location,
    location_prefix,
    rn,
    last3_count_budget,
    last3_min_budget,
    budget_percent_rank
FROM enriched
