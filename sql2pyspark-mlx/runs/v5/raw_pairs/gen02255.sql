WITH joined AS (
    -- Attach customers attributes
    SELECT
        src.*,
        p.age,
        p.name
    FROM subscriptions src
    INNER JOIN customers p
        ON src.customer_id <=> p.id
    WHERE p.is_active
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY status, plan ORDER BY age DESC NULLS LAST) AS rank_in_group
    FROM joined
    QUALIFY rank_in_group = 1
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM deduped e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.start_date BETWEEN sub.start_date AND sub.end_date
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.age AS customer_age,
        p.city,
        p.email
    FROM with_window e
    LEFT JOIN customers p
        ON e.customer_id <=> p.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(subscription_id / SUM(subscription_id) OVER (PARTITION BY customer_id, city), 4) AS subscription_id_share,
        NTILE(4) OVER (PARTITION BY customer_id ORDER BY customer_age, id, subscription_id) AS customer_age_bucket
    FROM with_dims
),
metrics_subscriptions AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        end_date,
        status,
        name,
        rank_in_group,
        subscription_id,
        subscription_plan,
        subscription_monthly_fee,
        customer_age,
        subscription_id_share,
        SUM(customer_age_bucket) OVER (PARTITION BY plan ORDER BY customer_age_bucket, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_customer_age_bucket,
        LAST_VALUE(subscription_plan, TRUE) OVER (PARTITION BY customer_id, city ORDER BY customer_age, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_subscription_plan
    FROM metrics
),
daily AS (
    -- Grouped metrics
    SELECT
        CASE WHEN subscription_monthly_fee >= 191 THEN 'LARGE' ELSE 'SMALL' END AS subscription_monthly_fee_tier,
        SUM(CASE WHEN subscription_plan = 'enterprise' THEN customer_age ELSE 0 END) AS enterprise_customer_age,
        PERCENTILE_APPROX(subscription_monthly_fee, 0.95) AS subscription_monthly_fee_p95,
        PERCENTILE_APPROX(customer_age, 0.5) AS customer_age_p50
    FROM metrics_subscriptions
    GROUP BY CASE WHEN subscription_monthly_fee >= 191 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    subscription_monthly_fee_tier,
    subscription_monthly_fee_p95,
    customer_age_p50
FROM daily;
