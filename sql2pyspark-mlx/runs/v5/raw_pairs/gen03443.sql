WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(email) email_lower,
        LENGTH(email) email_length
    FROM customers
    WHERE signup_date > TO_DATE('2023-07-12')
),
latest_match AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.subscription_plan,
        latest.subscription_start_date
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.plan AS subscription_plan, c.start_date AS subscription_start_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.start_date <= s.signup_date
            ORDER BY c.start_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
base_customers AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(credit_limit, 0) credit_limit_filled,
        UPPER(TRIM(name)) name_normalized,
        COALESCE(subscription_plan, 'N/A') subscription_plan_filled
    FROM latest_match
    WHERE signup_date >= DATE '2023-08-12'
      AND email_length <= 77
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        email,
        country,
        segment,
        credit_limit,
        age,
        email_lower,
        subscription_start_date,
        subscription_plan_filled,
        ROW_NUMBER() OVER (PARTITION BY subscription_plan ORDER BY age, id) event_seq,
        AVG(email_length) OVER (PARTITION BY segment ORDER BY subscription_start_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) email_length_avg_5,
        CORR(credit_limit, age) OVER (PARTITION BY segment) corr_credit_limit_age,
        LAG(age, 1) OVER (ORDER BY signup_date, id) prev_age,
        LAST_VALUE(subscription_plan, TRUE) OVER (PARTITION BY city ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_subscription_plan
    FROM base_customers
)
SELECT
    id,
    name,
    email,
    country,
    segment,
    credit_limit,
    email_lower,
    subscription_plan_filled,
    email_length_avg_5,
    CASE WHEN name RLIKE '^(Jack Taylor|bob smith)$' THEN 'EXTENDED' WHEN name RLIKE '^(Alice Wong|  Carol Diaz)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS name_zone
FROM scored
ORDER BY email_length_avg_5 DESC NULLS FIRST;
