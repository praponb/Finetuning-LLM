WITH enriched AS (
    -- Attach customers attributes
    SELECT src.*, p.country
    FROM subscriptions src
    LEFT JOIN customers p
        ON src.customer_id <=> p.id
    WHERE p.email NOT LIKE '%@yahoo.com'
),
joined AS (
    -- Attach customers attributes
    SELECT s.*, p.signup_date
    FROM enriched s
    INNER JOIN customers p
        ON s.customer_id = p.id
),
enriched_subscriptions AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        start_date,
        status,
        country,
        RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee NULLS LAST) AS monthly_fee_rank,
        AVG(monthly_fee) OVER (ORDER BY monthly_fee, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_6_prior,
        STDDEV_SAMP(monthly_fee) OVER (PARTITION BY plan) AS plan_stddev_samp_monthly_fee,
        COUNT(monthly_fee) OVER (PARTITION BY plan) AS plan_count_monthly_fee
    FROM joined
),
enriched_subscriptions_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        start_date,
        monthly_fee_rank,
        monthly_fee_avg_6_prior,
        plan_stddev_samp_monthly_fee,
        AVG(monthly_fee_avg_6_prior) OVER (PARTITION BY customer_id ORDER BY monthly_fee_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_monthly_fee_avg_6_prior,
        MIN(monthly_fee_avg_6_prior) OVER (PARTITION BY customer_id ORDER BY monthly_fee_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_monthly_fee_avg_6_prior,
        SUM(monthly_fee_avg_6_prior) OVER (PARTITION BY customer_id ORDER BY monthly_fee_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_monthly_fee_avg_6_prior,
        COUNT(monthly_fee_avg_6_prior) OVER (PARTITION BY customer_id ORDER BY monthly_fee_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_monthly_fee_avg_6_prior
    FROM enriched_subscriptions
)
SELECT
    id,
    customer_id,
    monthly_fee_avg_6_prior,
    plan_stddev_samp_monthly_fee,
    running_min_monthly_fee_avg_6_prior,
    running_sum_monthly_fee_avg_6_prior,
    running_count_monthly_fee_avg_6_prior
FROM enriched_subscriptions_2
ORDER BY monthly_fee_avg_6_prior;
