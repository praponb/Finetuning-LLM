WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        monthly_fee,
        status,
        LOWER(plan) plan_lower
    FROM subscriptions
    WHERE start_date >= TO_DATE('2023-05-18')
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        monthly_fee,
        status,
        SUM(monthly_fee) OVER w_running last4_sum_monthly_fee,
        COUNT(monthly_fee) OVER w_running last4_count_monthly_fee,
        MIN(monthly_fee) OVER w_running last4_min_monthly_fee,
        AVG(monthly_fee) OVER w_running last4_avg_monthly_fee,
        NTILE(3) OVER (ORDER BY monthly_fee DESC, id) monthly_fee_bucket
    FROM base
    WINDOW w_running AS (PARTITION BY status, customer_id ORDER BY monthly_fee, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        status,
        RANK() OVER w_running last4_min_monthly_fee_rank,
        AVG(last4_count_monthly_fee) OVER (PARTITION BY status ORDER BY last4_min_monthly_fee, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) last4_count_monthly_fee_avg_6_prior,
        LAG(monthly_fee_bucket, 2, 0) OVER w_hist prev_monthly_fee_bucket
    FROM scored
    WINDOW w_running AS (PARTITION BY status ORDER BY last4_min_monthly_fee NULLS FIRST), w_hist AS (PARTITION BY status ORDER BY last4_count_monthly_fee, id)
),
enriched_subscriptions AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(last4_count_monthly_fee_avg_6_prior) OVER w_grp last3_max_last4_count_monthly_fee_avg_6_prior,
        SUM(last4_count_monthly_fee_avg_6_prior) OVER w_grp last3_sum_last4_count_monthly_fee_avg_6_prior,
        MIN(last4_count_monthly_fee_avg_6_prior) OVER w_grp last3_min_last4_count_monthly_fee_avg_6_prior,
        AVG(last4_count_monthly_fee_avg_6_prior) OVER w_grp last3_avg_last4_count_monthly_fee_avg_6_prior,
        LEAD(prev_monthly_fee_bucket, 1) OVER w_part next_prev_monthly_fee_bucket
    FROM enriched
    WINDOW w_grp AS (PARTITION BY status ORDER BY last4_count_monthly_fee_avg_6_prior, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY status ORDER BY last4_count_monthly_fee_avg_6_prior, id)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY last3_max_last4_count_monthly_fee_avg_6_prior DESC NULLS FIRST, id) AS rank_in_group
    FROM enriched_subscriptions
    QUALIFY rank_in_group = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        status,
        prev_monthly_fee_bucket,
        last3_max_last4_count_monthly_fee_avg_6_prior,
        last3_sum_last4_count_monthly_fee_avg_6_prior,
        last3_avg_last4_count_monthly_fee_avg_6_prior,
        rank_in_group,
        SUM(prev_monthly_fee_bucket) OVER (PARTITION BY status) status_sum_prev_monthly_fee_bucket,
        NTILE(5) OVER w_hist last3_max_last4_count_monthly_fee_avg_6_prior_bucket,
        AVG(last4_count_monthly_fee_avg_6_prior) OVER (PARTITION BY status ORDER BY last4_min_monthly_fee_rank, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) last4_count_monthly_fee_avg_6_prior_avg_5_prior
    FROM deduped
    WHERE last3_avg_last4_count_monthly_fee_avg_6_prior >= 6.9
    WINDOW w_hist AS (ORDER BY last3_max_last4_count_monthly_fee_avg_6_prior DESC, id)
),
enriched_subscriptions_2 AS (
    -- Ranking and gap features
    SELECT
        id,
        prev_monthly_fee_bucket,
        last3_sum_last4_count_monthly_fee_avg_6_prior,
        last3_avg_last4_count_monthly_fee_avg_6_prior,
        rank_in_group,
        status_sum_prev_monthly_fee_bucket,
        last3_max_last4_count_monthly_fee_avg_6_prior_bucket,
        LEAD(last3_max_last4_count_monthly_fee_avg_6_prior_bucket, 1, 0) OVER (PARTITION BY status ORDER BY last3_avg_last4_count_monthly_fee_avg_6_prior, id) next_last3_max_last4_count_monthly_fee_avg_6_prior_bucket,
        AVG(last3_max_last4_count_monthly_fee_avg_6_prior) OVER (PARTITION BY status ORDER BY last4_count_monthly_fee_avg_6_prior_avg_5_prior, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_max_last4_count_monthly_fee_avg_6_prior_avg_2,
        NTILE(5) OVER (PARTITION BY status ORDER BY last3_sum_last4_count_monthly_fee_avg_6_prior, id) last3_sum_last4_count_monthly_fee_avg_6_prior_bucket,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY last3_sum_last4_count_monthly_fee_avg_6_prior DESC, id) event_seq
    FROM metrics
)
SELECT
    id,
    prev_monthly_fee_bucket,
    last3_sum_last4_count_monthly_fee_avg_6_prior,
    rank_in_group,
    last3_max_last4_count_monthly_fee_avg_6_prior_bucket,
    next_last3_max_last4_count_monthly_fee_avg_6_prior_bucket,
    last3_sum_last4_count_monthly_fee_avg_6_prior_bucket
FROM enriched_subscriptions_2
ORDER BY prev_monthly_fee_bucket DESC
