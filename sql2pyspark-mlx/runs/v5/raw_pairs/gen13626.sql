WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date, id) AS rank_in_group
        FROM subscriptions
    ) t
    WHERE rank_in_group = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        rank_in_group,
        AVG(monthly_fee) OVER (PARTITION BY plan ORDER BY monthly_fee, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_5,
        SUM(monthly_fee) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee,
        LAG(monthly_fee, 2, 0) OVER (PARTITION BY status ORDER BY rank_in_group, id) AS prev_monthly_fee,
        LAG(monthly_fee, 1, 0) OVER (PARTITION BY status ORDER BY start_date, id) AS prev_monthly_fee_2,
        NTILE(4) OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC, id) AS monthly_fee_bucket
    FROM deduped
),
scored_subscriptions AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(monthly_fee_avg_5) OVER (PARTITION BY plan, status ORDER BY prev_monthly_fee_2, id) AS next_monthly_fee_avg_5,
        LEAD(monthly_fee) OVER (PARTITION BY plan, status ORDER BY prev_monthly_fee_2, id) AS next_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY plan, status ORDER BY prev_monthly_fee_2, id) AS event_seq,
        SUM(cumulative_monthly_fee) OVER (PARTITION BY plan ORDER BY monthly_fee_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cumulative_monthly_fee
    FROM scored
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        end_date,
        monthly_fee,
        rank_in_group,
        monthly_fee_avg_5,
        cumulative_monthly_fee,
        monthly_fee_bucket,
        next_monthly_fee_avg_5,
        next_monthly_fee,
        cumulative_cumulative_monthly_fee,
        monthly_fee - monthly_fee_avg_5 AS monthly_fee_minus_monthly_fee_avg_5
    FROM scored_subscriptions
    WHERE monthly_fee_bucket >= 6
),
report AS (
    SELECT
        id,
        plan,
        monthly_fee,
        rank_in_group,
        cumulative_monthly_fee,
        monthly_fee_bucket,
        next_monthly_fee_avg_5,
        next_monthly_fee,
        cumulative_cumulative_monthly_fee
    FROM cleaned
    ORDER BY id
)
SELECT
    id,
    rank_in_group,
    cumulative_monthly_fee,
    monthly_fee_bucket,
    next_monthly_fee_avg_5,
    next_monthly_fee,
    cumulative_cumulative_monthly_fee
FROM report
ORDER BY next_monthly_fee_avg_5 DESC NULLS LAST
