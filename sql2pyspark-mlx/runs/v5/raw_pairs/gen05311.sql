WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(plan) plan_upper,
        LAST_DAY(end_date) end_date_month_end
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        start_date,
        monthly_fee,
        status,
        plan_upper,
        SUM(monthly_fee) OVER (PARTITION BY status) status_sum_monthly_fee,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY customer_id), 4) monthly_fee_share,
        LAST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY plan ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_monthly_fee
    FROM base
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        start_date,
        monthly_fee,
        plan_upper,
        status_sum_monthly_fee,
        monthly_fee_share,
        DATE_SUB(start_date, 8) start_date_minus_8d,
        monthly_fee_share - last_known_monthly_fee monthly_fee_share_minus_last_known_monthly_fee
    FROM scored
    WHERE customer_id IS NOT NULL
      AND status <> 'PAUSED'
),
scored_subscriptions AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(5) OVER w_ordered monthly_fee_share_minus_last_known_monthly_fee_bucket,
        ROW_NUMBER() OVER w_grp seq_no,
        PERCENT_RANK() OVER w_ordered_2 monthly_fee_percent_rank
    FROM cleaned
    WINDOW w_ordered AS (PARTITION BY customer_id ORDER BY monthly_fee_share_minus_last_known_monthly_fee, id), w_grp AS (PARTITION BY customer_id ORDER BY start_date DESC, id), w_ordered_2 AS (PARTITION BY customer_id ORDER BY monthly_fee DESC NULLS LAST)
),
stats AS (
    -- Grouped metrics
    SELECT
        CASE WHEN monthly_fee_percent_rank >= 0.65 THEN 'HIGH' ELSE 'LOW' END AS monthly_fee_percent_rank_tier,
        seq_no,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS cnt,
        COUNT(DISTINCT customer_id) AS distinct_customer_ids,
        MAX(monthly_fee_share_minus_last_known_monthly_fee) AS max_monthly_fee_share_minus_last_known_monthly_fee,
        APPROX_PERCENTILE(monthly_fee_share_minus_last_known_monthly_fee_bucket, ARRAY(0.25, 0.5, 0.75)) AS monthly_fee_share_minus_last_known_monthly_fee_bucket_quartiles,
        PERCENT_RANK() OVER (PARTITION BY CASE WHEN monthly_fee_percent_rank >= 0.65 THEN 'HIGH' ELSE 'LOW' END ORDER BY COUNT(DISTINCT id) DESC) AS distinct_ids_percent_rank
    FROM scored_subscriptions
    GROUP BY monthly_fee_percent_rank_tier, seq_no
)
SELECT
    monthly_fee_percent_rank_tier,
    seq_no,
    distinct_ids,
    distinct_customer_ids,
    monthly_fee_share_minus_last_known_monthly_fee_bucket_quartiles,
    distinct_ids_percent_rank
FROM stats;
