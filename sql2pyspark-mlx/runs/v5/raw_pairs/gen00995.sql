WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(budget * 0.9, 2) AS adjusted_budget,
        COALESCE(dept_name, 'UNKNOWN') AS resolved_dept_name
    FROM departments
    WHERE dept_name IS DISTINCT FROM 'HR'
      AND dept_name IS NOT NULL
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        dept_name,
        location,
        budget,
        adjusted_budget,
        CEIL(budget) AS budget_whole,
        COALESCE(location, 'N/A') AS location_filled
    FROM filtered
    WHERE location IS DISTINCT FROM 'Berlin'
      AND location NOT IN ('Singapore', 'Bangkok', 'London')
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget ASC NULLS LAST, id ASC) = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN dept_name IN ('Sales', 'Legal') THEN 'Group A' ELSE 'Other' END AS dept_name_group,
        COALESCE(location, 'DIRECT') AS location_filled_2
    FROM ranked
    WHERE budget < 3568000
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(adjusted_budget) OVER (PARTITION BY location) AS location_avg_adjusted_budget,
        MIN(budget_whole) OVER (PARTITION BY dept_name) AS dept_name_min_budget_whole,
        PERCENT_RANK() OVER (ORDER BY adjusted_budget ASC NULLS LAST) AS adjusted_budget_percent_rank,
        NTILE(4) OVER (ORDER BY budget_whole DESC, id ASC) AS budget_whole_bucket,
        DENSE_RANK() OVER (PARTITION BY location, dept_name ORDER BY budget ASC) AS budget_dense_rank
    FROM valid
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND NOT x.is_remote
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(location_avg_adjusted_budget) OVER (PARTITION BY dept_name ORDER BY budget_whole_bucket ASC, id ASC) AS prev_location_avg_adjusted_budget,
        LAG(budget_whole) OVER (PARTITION BY dept_name ORDER BY budget_whole_bucket ASC, id ASC) AS prev_budget_whole,
        LEAD(budget_whole) OVER (PARTITION BY dept_name ORDER BY budget_whole_bucket ASC, id ASC) AS next_budget_whole,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget_whole_bucket ASC, id ASC) AS seq_no
    FROM eligible
)
-- Multi-level aggregation
SELECT
    location,
    CASE WHEN next_budget_whole >= 73 THEN 'HIGH' ELSE 'LOW' END AS next_budget_whole_tier,
    ROUND(AVG(next_budget_whole), 2) AS avg_next_budget_whole,
    MAX(adjusted_budget) FILTER (WHERE dept_name = 'Engineering') AS engineering_max_adjusted_budget,
    AVG(budget_whole) AS avg_budget_whole,
    ROUND(SUM(next_budget_whole) / NULLIF(COUNT(CASE WHEN dept_name = 'HR' THEN 1 END), 0), 2) AS next_budget_whole_per_hr,
    SUM(CASE WHEN dept_name = 'Sales' THEN budget ELSE 0 END) AS sales_budget
FROM metrics
GROUP BY location, next_budget_whole_tier
ORDER BY next_budget_whole_per_hr ASC
