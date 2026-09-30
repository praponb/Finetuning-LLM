WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS event_seq,
        LAST_VALUE(budget, TRUE) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_budget,
        SUM(budget) OVER w_running AS location_sum_budget,
        PERCENT_RANK() OVER w_grp AS budget_percent_rank
    FROM departments
    WINDOW w_running AS (PARTITION BY location), w_grp AS (PARTITION BY dept_name ORDER BY budget)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(location_sum_budget) OVER (PARTITION BY location ORDER BY budget_percent_rank, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS location_sum_budget_avg_3_prior,
        NTILE(4) OVER (PARTITION BY location ORDER BY event_seq, id) AS event_seq_bucket,
        DENSE_RANK() OVER w_hist AS budget_dense_rank,
        NTILE(10) OVER (PARTITION BY dept_name ORDER BY last_known_budget DESC, id) AS last_known_budget_bucket,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY event_seq DESC, id) AS event_seq_2
    FROM enriched
    WINDOW w_hist AS (PARTITION BY dept_name ORDER BY budget NULLS LAST)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.level = 'L3'
        )
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.hire_date
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(budget_percent_rank) OVER (PARTITION BY dept_name) AS dept_name_avg_budget_percent_rank,
        RANK() OVER (PARTITION BY location, dept_name ORDER BY last_known_budget DESC) AS last_known_budget_rank,
        AVG(budget) OVER (PARTITION BY location) AS location_avg_budget
    FROM with_latest
),
daily AS (
    -- Grouped metrics
    SELECT
        dept_name,
        DATE_TRUNC('MONTH', hire_date) AS activity_month,
        AVG(event_seq_2) FILTER (WHERE location = 'Chiang Mai') AS chiang_mai_avg_event_seq_2,
        PERCENTILE_APPROX(last_known_budget, ARRAY(0.25, 0.5, 0.75)) AS last_known_budget_quartiles,
        ROUND(SUM(event_seq_bucket) / NULLIF(COUNT(CASE WHEN dept_name = 'Finance' THEN 1 END), 0), 2) AS event_seq_bucket_per_finance,
        MAX(budget) FILTER (WHERE location = 'Tokyo') AS tokyo_max_budget,
        RANK() OVER (PARTITION BY dept_name ORDER BY MAX(budget) FILTER (WHERE location = 'Tokyo') DESC) AS tokyo_max_budget_rank
    FROM scored
    GROUP BY dept_name, DATE_TRUNC('MONTH', hire_date)
)
SELECT
    dept_name,
    activity_month,
    chiang_mai_avg_event_seq_2,
    last_known_budget_quartiles,
    event_seq_bucket_per_finance,
    tokyo_max_budget,
    tokyo_max_budget_rank
FROM daily
ORDER BY tokyo_max_budget DESC NULLS LAST
