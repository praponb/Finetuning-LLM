WITH valid AS (
    SELECT *, CASE WHEN budget >= 4472000 THEN 'High' WHEN budget >= 1427000 THEN 'Medium' ELSE 'Low' END AS budget_tier
    FROM departments
    WHERE dept_name IS NOT NULL
      AND location NOT IN ('London', 'New York', 'Bangkok')
),
kept AS (
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND MONTH(c.hire_date) IN (1, 10, 11)
        )
),
metrics AS (
    SELECT
        *,
        MIN(budget) OVER (PARTITION BY dept_name) AS dept_name_min_budget,
        NTILE(5) OVER w_recent AS budget_bucket,
        LAG(budget, 1) OVER w_recent AS prev_budget
    FROM kept
    WINDOW w_recent AS (ORDER BY budget, id)
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY dept_name ORDER BY dept_name_min_budget) AS rank_in_group
        FROM metrics
    ) ranked
    WHERE rank_in_group = 1
),
enriched AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        budget_tier,
        dept_name_min_budget,
        budget_bucket,
        prev_budget,
        rank_in_group,
        DENSE_RANK() OVER w_ordered AS budget_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) AS event_seq,
        ROUND(prev_budget / SUM(prev_budget) OVER (PARTITION BY dept_name), 4) AS prev_budget_share
    FROM ranked
    WINDOW w_ordered AS (PARTITION BY dept_name ORDER BY budget NULLS LAST)
),
remaining_keys AS (
    SELECT id, location
    FROM enriched
    INTERSECT ALL
    SELECT id, location
    FROM departments
    WHERE budget <= 1611000
),
kept_rows AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.location = k.location
),
metrics_departments AS (
    SELECT
        *,
        STDDEV_SAMP(budget_bucket) OVER (PARTITION BY location, dept_name) AS location_stddev_samp_budget_bucket,
        CUME_DIST() OVER (ORDER BY budget_dense_rank DESC NULLS FIRST) AS budget_dense_rank_cume_dist,
        RANK() OVER (PARTITION BY dept_name ORDER BY budget_dense_rank) AS budget_dense_rank_rank,
        DENSE_RANK() OVER (ORDER BY dept_name_min_budget NULLS LAST) AS dept_name_min_budget_dense_rank,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY dept_name, location), 4) AS event_seq_share
    FROM kept_rows
)
SELECT
    location,
    CASE WHEN location_stddev_samp_budget_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END AS location_stddev_samp_budget_bucket_tier,
    COUNT_IF(location = 'Chiang Mai') AS chiang_mai_events,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list,
    COUNT(*) FILTER (WHERE dept_name = 'Sales') AS sales_rows
FROM metrics_departments
GROUP BY location, CASE WHEN location_stddev_samp_budget_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END
