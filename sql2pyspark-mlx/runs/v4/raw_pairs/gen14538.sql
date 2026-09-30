WITH with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.credit_limit,
        c.country
    FROM subscriptions a
    LEFT JOIN customers c
        ON a.customer_id = c.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        c.signup_date,
        c.email
    FROM with_dims s
    JOIN customers c
        ON s.customer_id = c.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(monthly_fee, 1) OVER (PARTITION BY customer_id ORDER BY signup_date, id) AS prev_monthly_fee,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status), 4) AS monthly_fee_share
    FROM enriched
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(plan) AS plan_length,
        prev_monthly_fee - credit_limit AS prev_monthly_fee_minus_credit_limit
    FROM with_windows
    WHERE customer_id IS NULL
)
SELECT
    id,
    start_date,
    end_date,
    monthly_fee,
    status,
    credit_limit,
    country,
    signup_date,
    prev_monthly_fee
FROM filtered
ORDER BY credit_limit
