WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        monthly_fee,
        TRIM(plan) AS clean_plan,
        CASE WHEN plan IN ('standard', 'enterprise') THEN 'Priority' ELSE 'Other' END AS plan_group
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        start_date,
        clean_plan,
        LAG(monthly_fee, 1) OVER (PARTITION BY plan, customer_id ORDER BY monthly_fee, id) AS prev_monthly_fee,
        AVG(monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_5_prior
    FROM filtered
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM with_windows e
    JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.start_date >= sub.start_date
       AND e.start_date < sub.end_date + INTERVAL 14 DAYS
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        d.country,
        d.email,
        d.credit_limit
    FROM with_window src
    LEFT JOIN customers d
        ON src.customer_id = d.id
    WHERE d.segment IN ('Consumer', 'SMB')
),
with_dims_subscriptions AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.name,
        p.signup_date
    FROM with_dims e
    JOIN customers p
        ON e.customer_id = p.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.*,
        c.segment,
        c.signup_date AS customer_signup_date,
        c.country AS customer_country
    FROM with_dims_subscriptions src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(prev_monthly_fee) IGNORE NULLS OVER (PARTITION BY segment, plan ORDER BY monthly_fee, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prev_monthly_fee,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY subscription_id NULLS FIRST) AS subscription_id_dense_rank,
        LAG(monthly_fee_avg_5_prior, 1, 0) OVER (ORDER BY credit_limit, id, subscription_id) AS prev_monthly_fee_avg_5_prior,
        NTILE(4) OVER (PARTITION BY segment ORDER BY prev_monthly_fee DESC, id, subscription_id) AS prev_monthly_fee_bucket
    FROM enriched
)
SELECT
    s.id,
    s.customer_id,
    s.clean_plan,
    s.prev_monthly_fee,
    s.monthly_fee_avg_5_prior,
    s.subscription_id,
    s.plan,
    s.monthly_fee,
    s.country,
    CASE WHEN plan RLIKE '^(premium|enterprise)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS plan_zone,
    (SELECT AVG(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS avg_account_balance
FROM metrics s
ORDER BY customer_id DESC
