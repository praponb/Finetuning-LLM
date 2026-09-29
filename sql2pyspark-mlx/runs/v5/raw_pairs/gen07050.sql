WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        end_date,
        monthly_fee,
        status,
        DATE_ADD(end_date, 44) end_date_plus_44d,
        LAST_DAY(end_date) end_date_month_end
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        end_date,
        monthly_fee,
        end_date_month_end
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date_plus_44d, id) AS rn
        FROM base
    ) x
    WHERE rn = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        monthly_fee,
        AVG(monthly_fee) OVER w_recent running_avg_monthly_fee,
        SUM(monthly_fee) OVER w_recent running_sum_monthly_fee,
        MAX(monthly_fee) OVER w_recent running_max_monthly_fee
    FROM latest
    WINDOW w_recent AS (ORDER BY end_date_month_end, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        monthly_fee,
        running_sum_monthly_fee,
        running_max_monthly_fee,
        GREATEST(running_avg_monthly_fee, running_max_monthly_fee) greatest_running_avg_monthly_fee_running_max_monthly_fee
    FROM enriched
    WHERE monthly_fee IS NOT NULL
),
enriched_subscriptions AS (
    -- Ranking and gap features
    SELECT
        id,
        monthly_fee,
        greatest_running_avg_monthly_fee_running_max_monthly_fee,
        LEAD(monthly_fee) OVER (ORDER BY monthly_fee, id) next_monthly_fee,
        LAG(monthly_fee) OVER (ORDER BY monthly_fee, id) prev_monthly_fee,
        LEAD(running_max_monthly_fee) OVER (ORDER BY monthly_fee, id) next_running_max_monthly_fee,
        ROW_NUMBER() OVER (ORDER BY monthly_fee, id) event_seq,
        LAST_VALUE(monthly_fee) IGNORE NULLS OVER (ORDER BY running_sum_monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_monthly_fee
    FROM cleaned
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        next_monthly_fee,
        next_running_max_monthly_fee,
        SUM(event_seq) OVER (ORDER BY last_known_monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_event_seq,
        AVG(monthly_fee) OVER (ORDER BY monthly_fee, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) monthly_fee_avg_6_prior
    FROM enriched_subscriptions
    WHERE prev_monthly_fee > 130
),
cleaned_subscriptions AS (
    -- Keep recent, valid rows
    SELECT
        id,
        next_monthly_fee,
        next_running_max_monthly_fee,
        running_event_seq,
        monthly_fee_avg_6_prior,
        ROUND(monthly_fee_avg_6_prior * 0.85, 2) monthly_fee_avg_6_prior_scaled
    FROM metrics
    WHERE next_running_max_monthly_fee IS NOT NULL
)
SELECT
    id,
    next_monthly_fee,
    next_running_max_monthly_fee,
    running_event_seq,
    monthly_fee_avg_6_prior
FROM cleaned_subscriptions
ORDER BY next_monthly_fee DESC NULLS LAST
