WITH filtered AS (
    SELECT *, CASE WHEN location IN ('Chiang Mai', 'Tokyo') THEN 'Tier 1' ELSE 'Other' END AS location_group
    FROM departments
    WHERE location != 'Bangkok'
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS latest_rank
        FROM filtered
    ) t
    WHERE latest_rank = 1
),
remaining_keys AS (
    SELECT dept_name, location
    FROM ranked
    EXCEPT ALL
    SELECT dept_name, location
    FROM departments
    WHERE location IN ('Tokyo', 'Chiang Mai', 'New York')
),
reconciled AS (
    SELECT s.*
    FROM ranked s
    INNER JOIN remaining_keys k ON s.dept_name = k.dept_name AND s.location = k.location
),
enriched AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY location ORDER BY budget DESC NULLS FIRST) AS budget_percent_rank,
        RANK() OVER w_ordered AS budget_rank,
        COUNT(budget) OVER w_recent AS dept_name_count_budget,
        COUNT(budget) OVER (PARTITION BY dept_name) AS dept_name_count_budget_2
    FROM reconciled
    WINDOW w_ordered AS (ORDER BY budget DESC), w_recent AS (PARTITION BY dept_name, location)
),
enriched_departments AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY dept_name ORDER BY budget_rank NULLS FIRST) AS budget_rank_dense_rank,
        MAX(budget_percent_rank) OVER (PARTITION BY location) AS location_max_budget_percent_rank,
        ROUND(dept_name_count_budget / SUM(dept_name_count_budget) OVER (PARTITION BY dept_name), 4) AS dept_name_count_budget_share,
        CUME_DIST() OVER (ORDER BY budget_rank NULLS FIRST) AS budget_rank_cume_dist
    FROM enriched
)
SELECT
    id,
    dept_name,
    budget,
    location_group,
    budget_percent_rank,
    budget_rank,
    dept_name_count_budget,
    dept_name_count_budget_2,
    budget_rank_dense_rank,
    CASE WHEN dept_name RLIKE '^(Sales|HR)$' THEN 'PRIORITY' WHEN dept_name RLIKE '^(Engineering|Marketing)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS dept_name_zone
FROM enriched_departments
ORDER BY dept_name_zone DESC NULLS FIRST, budget
