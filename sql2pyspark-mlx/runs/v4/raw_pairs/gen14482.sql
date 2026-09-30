WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        email,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        age - credit_limit AS age_minus_credit_limit,
        LAST_DAY(signup_date) AS signup_date_month_end
    FROM customers
    WHERE city IS NOT NULL
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, segment ORDER BY signup_date DESC NULLS LAST, id) AS rank_in_group
    FROM filtered
    QUALIFY rank_in_group = 1
),
latest_match AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.plan,
        latest.subscription_monthly_fee
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.plan, c.monthly_fee AS subscription_monthly_fee
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.start_date <= s.signup_date_month_end
            ORDER BY c.start_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Ranking and gap features
SELECT
    *,
    SUM(age_minus_credit_limit) OVER (ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_age_minus_credit_limit,
    NTILE(3) OVER (PARTITION BY city ORDER BY age DESC, id) AS age_bucket,
    SUM(credit_limit) OVER (ORDER BY subscription_monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_credit_limit
FROM latest_match
