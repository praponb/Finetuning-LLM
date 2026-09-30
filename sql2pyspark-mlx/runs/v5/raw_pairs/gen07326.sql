WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        status,
        DAYOFWEEK(start_date) start_date_dayofweek,
        LENGTH(plan) plan_length
    FROM subscriptions
    WHERE start_date > DATE '2023-04-19'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(start_date_dayofweek, 1) OVER (PARTITION BY status, customer_id ORDER BY start_date_dayofweek, id) next_start_date_dayofweek,
        COUNT(start_date_dayofweek) OVER (PARTITION BY plan) plan_count_start_date_dayofweek,
        RANK() OVER (ORDER BY plan_length DESC) plan_length_rank
    FROM filtered
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM enriched
    QUALIFY RANK() OVER (PARTITION BY plan ORDER BY start_date NULLS FIRST) = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        plan,
        status,
        start_date_dayofweek,
        plan_length,
        plan_count_start_date_dayofweek,
        plan_length_rank
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status ORDER BY plan_count_start_date_dayofweek DESC) AS row_num
        FROM deduped
    ) x
    WHERE row_num = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        start_date_dayofweek,
        plan_count_start_date_dayofweek,
        plan_length_rank,
        MAX(plan_length) OVER (PARTITION BY plan) plan_max_plan_length,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY plan_length DESC, id) event_seq,
        NTILE(10) OVER (PARTITION BY plan ORDER BY plan_count_start_date_dayofweek, id) plan_count_start_date_dayofweek_bucket,
        MAX(plan_length_rank) OVER (PARTITION BY plan) plan_max_plan_length_rank,
        SUM(plan_count_start_date_dayofweek) OVER (PARTITION BY plan, status ORDER BY start_date_dayofweek, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_plan_count_start_date_dayofweek
    FROM latest
    WHERE plan NOT IN ('premium', 'standard')
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(plan_count_start_date_dayofweek) OVER (ORDER BY plan_count_start_date_dayofweek, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) plan_count_start_date_dayofweek_avg_6_prior,
        ROW_NUMBER() OVER (ORDER BY plan_length_rank, id) seq_no
    FROM with_windows
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM metrics
    EXCEPT
    SELECT id
    FROM subscriptions
    WHERE start_date BETWEEN '2024-04-01' AND '2025-01-29'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN kept_keys k ON s.id = k.id
)
SELECT
    id,
    plan_count_start_date_dayofweek,
    plan_length_rank,
    plan_max_plan_length,
    event_seq,
    plan_count_start_date_dayofweek_bucket,
    cumulative_plan_count_start_date_dayofweek
FROM kept_rows
