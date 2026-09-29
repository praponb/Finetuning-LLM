WITH enriched AS (
    -- Attach customers attributes
    SELECT s.*, p.credit_limit
    FROM subscriptions s
    LEFT JOIN customers p
        ON s.customer_id = p.id
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM enriched src
    INNER JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.start_date BETWEEN sub.start_date AND sub.end_date
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY start_date DESC, id ASC, subscription_id ASC) AS row_seq,
        AVG(subscription_id) OVER (PARTITION BY customer_id ORDER BY end_date ASC, id ASC, subscription_id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS subscription_id_avg_2_prior,
        LAG(subscription_monthly_fee, 1) OVER (PARTITION BY status ORDER BY subscription_monthly_fee ASC, id ASC, subscription_id ASC) AS prev_subscription_monthly_fee,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY monthly_fee ASC) AS monthly_fee_dense_rank,
        LEAD(monthly_fee, 2, 0) OVER (ORDER BY subscription_id ASC, id ASC, subscription_id ASC) AS next_monthly_fee
    FROM matched
)
-- Running and moving aggregates
SELECT
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    status,
    credit_limit,
    subscription_id,
    subscription_plan,
    subscription_monthly_fee,
    row_seq,
    subscription_id_avg_2_prior,
    prev_subscription_monthly_fee,
    monthly_fee_dense_rank,
    next_monthly_fee,
    LAG(next_monthly_fee, 1) OVER (PARTITION BY status, customer_id ORDER BY monthly_fee_dense_rank ASC, id ASC, subscription_id ASC) AS prev_next_monthly_fee,
    FIRST_VALUE(subscription_id) IGNORE NULLS OVER (PARTITION BY status, customer_id ORDER BY subscription_id_avg_2_prior ASC, id ASC, subscription_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_subscription_id,
    NTILE(4) OVER (PARTITION BY status ORDER BY credit_limit ASC, id ASC, subscription_id ASC) AS credit_limit_bucket
FROM metrics;
