WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(location) AS clean_location
    FROM departments
    WHERE location IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(location) IGNORE NULLS OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_location,
        AVG(budget) OVER w_ordered AS budget_avg_6_prior
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY location ORDER BY budget, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND YEAR(c.hire_date) >= 2023
        )
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(budget AS INT) AS budget_int,
        ROUND(budget * 1.15, 2) AS adjusted_budget,
        COALESCE(location, 'NONE') AS location_filled
    FROM eligible
    WHERE location IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        location,
        budget,
        first_known_location,
        budget_avg_6_prior,
        location_filled,
        LEAD(budget_avg_6_prior) OVER w_recent AS next_budget_avg_6_prior,
        LEAD(budget) OVER w_recent AS next_budget,
        LAG(budget) OVER w_recent AS prev_budget,
        ROW_NUMBER() OVER w_recent AS seq_no
    FROM cleaned
    WINDOW w_recent AS (PARTITION BY location ORDER BY budget, id)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(next_budget) OVER (PARTITION BY location ORDER BY prev_budget, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS next_budget_avg_5,
        DENSE_RANK() OVER (PARTITION BY location ORDER BY prev_budget) AS prev_budget_dense_rank
    FROM with_windows
    WHERE next_budget >= 3168000
)
SELECT
    id,
    location,
    first_known_location,
    location_filled,
    next_budget_avg_6_prior,
    seq_no,
    next_budget_avg_5,
    CASE WHEN location RLIKE '^(Chiang Mai|Berlin)$' THEN 'CORE' WHEN location RLIKE '^(Bangkok|New York)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS location_zone
FROM enriched;
