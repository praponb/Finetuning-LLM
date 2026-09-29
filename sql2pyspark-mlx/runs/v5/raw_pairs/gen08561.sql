WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        monthly_fee,
        status,
        AVG(monthly_fee) OVER (PARTITION BY customer_id ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_2,
        SUM(monthly_fee) OVER (PARTITION BY plan, customer_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee
    FROM subscriptions
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.monthly_fee,
        src.status,
        b.city,
        b.credit_limit,
        b.email
    FROM metrics src
    LEFT JOIN customers b
        ON src.customer_id = b.id
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY monthly_fee DESC, id) AS latest_rank
    FROM enriched
    QUALIFY latest_rank = 1
),
metrics_subscriptions AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY status ORDER BY credit_limit DESC, id) AS credit_limit_bucket,
        LEAD(credit_limit, 1) OVER (PARTITION BY status, city ORDER BY credit_limit, id) AS next_credit_limit
    FROM deduped
),
with_dims AS (
    -- Attach customers attributes
    SELECT a.*, b.city AS customer_city
    FROM metrics_subscriptions a
    LEFT JOIN customers b
        ON a.customer_id = b.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        status,
        city,
        latest_rank,
        credit_limit_bucket,
        SUM(monthly_fee) OVER (PARTITION BY customer_city, city ORDER BY credit_limit_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee,
        LEAD(credit_limit, 2) OVER (PARTITION BY customer_city, city ORDER BY monthly_fee, id) AS next_credit_limit_2,
        AVG(credit_limit) OVER (PARTITION BY city ORDER BY credit_limit, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS credit_limit_avg_6_prior,
        SUM(next_credit_limit) OVER (PARTITION BY customer_city ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_next_credit_limit
    FROM with_dims
    WHERE city IN ('Bangkok', 'Chiang Mai', 'Tokyo')
),
enriched_subscriptions AS (
    -- Ranking and gap features
    SELECT
        id,
        status,
        latest_rank,
        credit_limit_bucket,
        running_monthly_fee,
        next_credit_limit_2,
        credit_limit_avg_6_prior,
        ROUND(credit_limit_avg_6_prior / SUM(credit_limit_avg_6_prior) OVER (PARTITION BY customer_id), 4) AS credit_limit_avg_6_prior_share,
        LAG(credit_limit_avg_6_prior, 2, 0) OVER (ORDER BY running_monthly_fee, id) AS prev_credit_limit_avg_6_prior,
        LEAD(running_monthly_fee, 1) OVER (ORDER BY running_monthly_fee, id) AS next_running_monthly_fee
    FROM with_windows
)
SELECT
    id,
    latest_rank,
    credit_limit_bucket,
    running_monthly_fee,
    next_credit_limit_2,
    prev_credit_limit_avg_6_prior,
    next_running_monthly_fee
FROM enriched_subscriptions;
