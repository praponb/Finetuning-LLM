WITH scored AS (
    SELECT
        id,
        dept_name,
        location,
        LAST_VALUE(location, TRUE) OVER w_hist AS last_known_location,
        SUM(budget) OVER (PARTITION BY location, dept_name ORDER BY budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget
    FROM departments
    WINDOW w_hist AS (PARTITION BY dept_name ORDER BY budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    SELECT dept_name
    FROM scored
    INTERSECT
    SELECT dept_name
    FROM departments
    WHERE location <> 'Chiang Mai'
),
kept_rows AS (
    SELECT s.*
    FROM scored s
    JOIN kept_keys k ON s.dept_name = k.dept_name
),
metrics AS (
    SELECT
        location,
        last_known_location,
        ROUND(running_budget / SUM(running_budget) OVER (PARTITION BY location), 4) AS running_budget_share,
        COUNT(running_budget) OVER (PARTITION BY dept_name) AS dept_name_count_running_budget
    FROM kept_rows
    WHERE dept_name IS NOT NULL
),
enriched AS (
    SELECT
        location,
        last_known_location,
        running_budget_share,
        CUME_DIST() OVER (PARTITION BY location ORDER BY running_budget_share DESC NULLS LAST) AS running_budget_share_cume_dist,
        SUM(dept_name_count_running_budget) OVER (PARTITION BY location) AS location_sum_dept_name_count_running_budget
    FROM metrics
),
metrics_departments AS (
    SELECT
        *,
        COUNT(running_budget_share) OVER w_grp AS location_count_running_budget_share,
        RANK() OVER w_recent AS running_budget_share_cume_dist_rank
    FROM enriched
    WINDOW w_grp AS (PARTITION BY location), w_recent AS (ORDER BY running_budget_share_cume_dist DESC)
),
base AS (
    SELECT
        *,
        CAST(location_sum_dept_name_count_running_budget AS DOUBLE) AS location_sum_dept_name_count_running_budget_num,
        COALESCE(running_budget_share, 0) AS running_budget_share_filled,
        COALESCE(location, 'DIRECT') AS location_filled
    FROM metrics_departments
    WHERE location_sum_dept_name_count_running_budget <> 44
      AND location IS NOT NULL
),
enriched_departments AS (
    SELECT
        last_known_location,
        running_budget_share,
        running_budget_share_cume_dist,
        location_count_running_budget_share,
        running_budget_share_filled,
        location_filled,
        DENSE_RANK() OVER (PARTITION BY location ORDER BY running_budget_share_filled DESC) AS running_budget_share_filled_dense_rank
    FROM base
)
SELECT
    last_known_location,
    running_budget_share,
    running_budget_share_cume_dist,
    running_budget_share_filled,
    location_filled
FROM enriched_departments
ORDER BY running_budget_share_filled ASC;
