WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT_WS('-', status, plan) AS status_plan_key
    FROM subscriptions
    WHERE end_date > DATE '2023-08-22'
      AND end_date < '2025-06-16'
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.name,
        c.credit_limit
    FROM cleaned a
    INNER JOIN customers c
        ON a.customer_id <=> c.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        end_date,
        monthly_fee,
        status_plan_key,
        name,
        credit_limit,
        LEAD(credit_limit, 2) OVER w_recent AS next_credit_limit,
        AVG(monthly_fee) OVER (ORDER BY monthly_fee, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_2,
        LAST_VALUE(credit_limit) IGNORE NULLS OVER (PARTITION BY plan ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credit_limit,
        SUM(credit_limit) OVER (PARTITION BY status ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_credit_limit
    FROM joined
    WINDOW w_recent AS (PARTITION BY plan, status ORDER BY start_date, id)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        end_date,
        monthly_fee,
        credit_limit,
        monthly_fee_avg_2,
        running_credit_limit,
        AVG(running_credit_limit) OVER w_grp AS running_credit_limit_avg_6_prior,
        AVG(next_credit_limit) OVER w_recent AS next_credit_limit_avg_5,
        SUM(running_credit_limit) OVER (PARTITION BY plan ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_credit_limit,
        AVG(running_credit_limit) OVER w_recent_2 AS running_credit_limit_avg_6_prior_2,
        LEAD(monthly_fee, 1, 0) OVER w_part AS next_monthly_fee
    FROM scored
    WHERE running_credit_limit BETWEEN 42502 AND 78065
    WINDOW w_grp AS (PARTITION BY plan ORDER BY running_credit_limit, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), w_recent AS (PARTITION BY plan ORDER BY monthly_fee, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), w_recent_2 AS (PARTITION BY plan ORDER BY monthly_fee_avg_2, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), w_part AS (ORDER BY next_credit_limit, id)
),
with_windows_subscriptions AS (
    -- Running and moving aggregates
    SELECT
        id,
        running_credit_limit,
        running_credit_limit_avg_6_prior,
        next_credit_limit_avg_5,
        running_running_credit_limit,
        DENSE_RANK() OVER (ORDER BY monthly_fee_avg_2 DESC NULLS LAST) AS monthly_fee_avg_2_dense_rank,
        SUM(running_credit_limit) OVER w_grp AS running_running_credit_limit_2,
        SUM(monthly_fee_avg_2) OVER (ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee_avg_2
    FROM with_windows
    WINDOW w_grp AS (ORDER BY next_monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_subscriptions_2 AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(running_running_credit_limit_2, 1, 0) OVER (ORDER BY cumulative_monthly_fee_avg_2, id) AS next_running_running_credit_limit_2,
        PERCENT_RANK() OVER (ORDER BY cumulative_monthly_fee_avg_2 DESC NULLS LAST) AS cumulative_monthly_fee_avg_2_percent_rank,
        AVG(next_credit_limit_avg_5) OVER (ORDER BY next_credit_limit_avg_5, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS next_credit_limit_avg_5_avg_6,
        SUM(running_running_credit_limit_2) OVER w_grp AS cumulative_running_running_credit_limit_2
    FROM with_windows_subscriptions
    WINDOW w_grp AS (ORDER BY running_credit_limit_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Running and moving aggregates
SELECT
    id,
    running_credit_limit,
    running_credit_limit_avg_6_prior,
    next_credit_limit_avg_5,
    running_running_credit_limit,
    monthly_fee_avg_2_dense_rank,
    cumulative_monthly_fee_avg_2,
    next_credit_limit_avg_5_avg_6,
    cumulative_running_running_credit_limit_2,
    LEAD(next_running_running_credit_limit_2, 1) OVER (ORDER BY running_credit_limit, id) AS next_next_running_running_credit_limit_2,
    ROW_NUMBER() OVER (ORDER BY running_credit_limit DESC, id) AS row_seq,
    CUME_DIST() OVER (ORDER BY monthly_fee_avg_2_dense_rank NULLS LAST) AS monthly_fee_avg_2_dense_rank_cume_dist,
    LAG(cumulative_monthly_fee_avg_2_percent_rank, 1) OVER (ORDER BY cumulative_monthly_fee_avg_2, id) AS prev_cumulative_monthly_fee_avg_2_percent_rank
FROM with_windows_subscriptions_2;
