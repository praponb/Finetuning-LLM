WITH valid AS (
    SELECT
        *,
        ROUND(budget, 2) AS budget_rounded,
        CONCAT_WS(',', dept_name, location) AS dept_name_location_key,
        COALESCE(dept_name, 'UNKNOWN') AS resolved_dept_name
    FROM departments
    WHERE location = 'Bangkok'
),
kept AS (
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.dept_id IN (5, 6, 16, 22)
        )
),
filtered AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        budget_rounded,
        dept_name_location_key,
        resolved_dept_name,
        LOWER(dept_name) AS dept_name_lower,
        COALESCE(dept_name, 'DIRECT') AS dept_name_clean
    FROM kept
    WHERE location IS NOT NULL
),
remaining_keys AS (
    SELECT id, dept_name
    FROM filtered
    EXCEPT DISTINCT
    SELECT id, dept_name
    FROM departments
    WHERE dept_name <> 'Sales'
),
kept_rows AS (
    SELECT s.*
    FROM filtered s
    JOIN remaining_keys k ON s.id = k.id AND s.dept_name = k.dept_name
),
scored AS (
    SELECT
        *,
        MIN(budget_rounded) OVER (PARTITION BY dept_name) AS dept_name_min_budget_rounded,
        MIN(budget) OVER (PARTITION BY dept_name, location) AS dept_name_min_budget,
        RANK() OVER w_recent AS budget_rank
    FROM kept_rows
    WINDOW w_recent AS (PARTITION BY dept_name ORDER BY budget NULLS FIRST)
),
enriched AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        budget_rounded,
        dept_name_location_key,
        resolved_dept_name,
        dept_name_lower,
        dept_name_clean,
        dept_name_min_budget_rounded,
        dept_name_min_budget,
        budget_rank,
        RANK() OVER (ORDER BY dept_name_min_budget_rounded DESC) AS dept_name_min_budget_rounded_rank,
        COUNT(budget_rank) OVER (PARTITION BY dept_name) AS dept_name_count_budget_rank
    FROM scored
),
matched_keys AS (
    SELECT dept_name, location
    FROM enriched
    EXCEPT ALL
    SELECT dept_name, location
    FROM departments
    WHERE dept_name IS NOT NULL
),
verified AS (
    SELECT s.*
    FROM enriched s
    JOIN matched_keys k ON s.dept_name = k.dept_name AND s.location = k.location
)
SELECT
    CASE WHEN dept_name_min_budget_rounded_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS dept_name_min_budget_rounded_rank_tier,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list,
    APPROX_PERCENTILE(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list_2
FROM verified
GROUP BY CASE WHEN dept_name_min_budget_rounded_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END
