WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT_WS('-', plan, status) plan_status_key
    FROM subscriptions
    WHERE end_date >= DATE '2023-09-03'
      AND customer_id IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        monthly_fee,
        status,
        plan_status_key,
        FIRST_VALUE(monthly_fee) IGNORE NULLS OVER (PARTITION BY customer_id, status ORDER BY end_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_monthly_fee,
        STDDEV_SAMP(monthly_fee) OVER (PARTITION BY customer_id) customer_id_stddev_samp_monthly_fee,
        SUM(monthly_fee) OVER (ORDER BY end_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_monthly_fee
    FROM base
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        monthly_fee,
        plan_status_key,
        first_known_monthly_fee,
        running_monthly_fee,
        ROW_NUMBER() OVER w_grp row_seq,
        SUM(first_known_monthly_fee) OVER w_running running_first_known_monthly_fee,
        SUM(running_monthly_fee) OVER (PARTITION BY plan, status) plan_sum_running_monthly_fee,
        SUM(monthly_fee) OVER w_running cumulative_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY status, plan ORDER BY first_known_monthly_fee DESC, id ASC) event_seq
    FROM scored
    WHERE customer_id_stddev_samp_monthly_fee BETWEEN 76 AND 233
    WINDOW w_grp AS (ORDER BY running_monthly_fee DESC, id ASC), w_running AS (ORDER BY monthly_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(plan_sum_running_monthly_fee AS DOUBLE) plan_sum_running_monthly_fee_str
    FROM with_windows
    WHERE cumulative_monthly_fee <= 435
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM filtered
    EXCEPT
    SELECT id
    FROM subscriptions
    WHERE MONTH(start_date) IN (5, 6, 11)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN matched_keys k ON s.id = k.id
)
-- Ranking and gap features
SELECT
    monthly_fee,
    first_known_monthly_fee,
    row_seq,
    running_first_known_monthly_fee,
    plan_sum_running_monthly_fee,
    cumulative_monthly_fee,
    PERCENT_RANK() OVER (ORDER BY cumulative_monthly_fee ASC NULLS LAST) cumulative_monthly_fee_percent_rank
FROM reconciled
