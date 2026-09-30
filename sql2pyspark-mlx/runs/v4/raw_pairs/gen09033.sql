WITH latest AS (
    SELECT *, RANK() OVER (PARTITION BY dept_name ORDER BY budget DESC) AS latest_rank
    FROM departments
    QUALIFY latest_rank = 1
),
scored AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY location ORDER BY budget NULLS FIRST) AS budget_percent_rank,
        NTILE(4) OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS budget_bucket
    FROM latest
),
matched_keys AS (
    SELECT dept_name
    FROM scored
    EXCEPT ALL
    SELECT dept_name
    FROM departments
    WHERE budget < 1177000
),
verified AS (
    SELECT s.*
    FROM scored s
    JOIN matched_keys k ON s.dept_name = k.dept_name
),
base AS (
    SELECT
        id,
        dept_name,
        location,
        budget_percent_rank,
        budget_bucket,
        CASE WHEN budget_percent_rank >= 0.63 THEN 'High' WHEN budget_percent_rank >= 0.16 THEN 'Medium' ELSE 'Low' END AS budget_percent_rank_tier
    FROM verified
    WHERE budget_bucket >= 4
),
not_flagged AS (
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.manager_id = 26
        )
),
scored_departments AS (
    SELECT
        *,
        AVG(budget_bucket) OVER (PARTITION BY location) AS location_avg_budget_bucket,
        SUM(budget_bucket) OVER (PARTITION BY location) AS location_sum_budget_bucket,
        DENSE_RANK() OVER (ORDER BY budget_bucket DESC NULLS FIRST) AS budget_bucket_dense_rank
    FROM not_flagged
),
cleaned AS (
    SELECT
        id,
        location,
        location_avg_budget_bucket,
        location_sum_budget_bucket,
        budget_bucket_dense_rank,
        CONCAT_WS(',', location, dept_name) AS location_dept_name_key,
        LOWER(location) AS location_lower
    FROM scored_departments
    WHERE location_avg_budget_bucket BETWEEN 3 AND 5
),
daily AS (
    SELECT
        CASE WHEN location_avg_budget_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END AS location_avg_budget_bucket_tier,
        CASE WHEN budget_bucket_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_bucket_dense_rank_tier,
        COUNT(*) FILTER (WHERE location = 'Chiang Mai') AS chiang_mai_rows,
        ROUND(SUM(location_sum_budget_bucket) / NULLIF(COUNT(CASE WHEN location = 'Singapore' THEN 1 END), 0), 2) AS location_sum_budget_bucket_per_singapore,
        COUNT(*) FILTER (WHERE location = 'Bangkok') AS bangkok_rows
    FROM cleaned
    GROUP BY GROUPING SETS ((CASE WHEN location_avg_budget_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN budget_bucket_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN location_avg_budget_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END))
)
SELECT
    budget_bucket_dense_rank_tier,
    location_sum_budget_bucket_per_singapore,
    bangkok_rows
FROM daily;
