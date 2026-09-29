WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(location)) AS location_title,
        COALESCE(dept_name, 'UNKNOWN') AS dept_name_filled
    FROM departments
    WHERE budget > 2665000
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget,
        LAG(budget, 1, 0) OVER (PARTITION BY location ORDER BY budget, id) AS prev_budget,
        RANK() OVER (PARTITION BY dept_name, location ORDER BY budget NULLS FIRST) AS budget_rank,
        RANK() OVER (PARTITION BY dept_name, location ORDER BY budget NULLS LAST) AS budget_rank_2,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS row_seq
    FROM filtered
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND LOWER(ch.full_name) LIKE '%ack%'
        )
),
filtered_departments AS (
    -- Keep recent, valid rows
    SELECT *, CAST(cumulative_budget AS STRING) AS cumulative_budget_num
    FROM eligible
    WHERE location IS NOT NULL
),
filtered_departments_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        dept_name_filled,
        cumulative_budget,
        prev_budget,
        row_seq,
        cumulative_budget_num,
        ROUND(budget_rank, 2) AS budget_rank_rounded,
        COALESCE(dept_name, 'DIRECT') AS dept_name_clean
    FROM filtered_departments
    WHERE dept_name IS NOT NULL
),
enriched_departments AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_name_filled,
        cumulative_budget,
        prev_budget,
        row_seq,
        cumulative_budget_num,
        budget_rank_rounded,
        dept_name_clean,
        DENSE_RANK() OVER (ORDER BY prev_budget DESC NULLS LAST) AS prev_budget_dense_rank,
        ROW_NUMBER() OVER w_recent AS row_seq_2,
        NTILE(10) OVER (ORDER BY row_seq, id) AS row_seq_bucket,
        NTILE(4) OVER (ORDER BY prev_budget DESC, id) AS prev_budget_bucket
    FROM filtered_departments_2
    WINDOW w_recent AS (ORDER BY cumulative_budget, id)
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN row_seq_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS row_seq_2_tier,
        CASE WHEN prev_budget_dense_rank >= 7 THEN 'HIGH' ELSE 'LOW' END AS prev_budget_dense_rank_tier,
        COUNT(*) AS cnt,
        COUNT(*) AS cnt_2,
        APPROX_PERCENTILE(prev_budget_bucket, ARRAY(0.25, 0.5, 0.75)) AS prev_budget_bucket_quartiles,
        MAX(prev_budget_dense_rank) AS max_prev_budget_dense_rank,
        PERCENTILE_APPROX(row_seq_bucket, ARRAY(0.25, 0.5, 0.75)) AS row_seq_bucket_quartiles,
        APPROX_PERCENTILE(cumulative_budget, 0.95) AS cumulative_budget_p95,
        PERCENT_RANK() OVER (PARTITION BY CASE WHEN row_seq_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT(*) DESC) AS cnt_percent_rank
    FROM enriched_departments
    GROUP BY CASE WHEN row_seq_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN prev_budget_dense_rank >= 7 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    row_seq_2_tier,
    prev_budget_dense_rank_tier,
    cnt,
    cnt_2,
    max_prev_budget_dense_rank,
    row_seq_bucket_quartiles,
    cumulative_budget_p95,
    cnt_percent_rank
FROM grouped
ORDER BY cumulative_budget_p95;
