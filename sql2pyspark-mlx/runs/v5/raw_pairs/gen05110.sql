WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(monthly_fee) OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_6,
        FIRST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY plan ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee
    FROM subscriptions
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, status
    FROM with_windows
    INTERSECT ALL
    SELECT id, status
    FROM subscriptions
    WHERE customer_id IN (1, 31, 36)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    JOIN matched_keys k ON s.id = k.id AND s.status = k.status
),
kept_keys AS (
    -- Set difference on keys
    SELECT status, plan
    FROM verified
    EXCEPT ALL
    SELECT status, plan
    FROM subscriptions
    WHERE monthly_fee < 150
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    JOIN kept_keys k ON s.status = k.status AND s.plan = k.plan
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.name,
        p.signup_date,
        p.country
    FROM reconciled a
    JOIN customers p
        ON a.customer_id = p.id
),
enriched_subscriptions AS (
    -- Attach customers attributes
    SELECT
        e.*,
        b.age,
        b.segment,
        b.city
    FROM enriched e
    JOIN customers b
        ON e.customer_id = b.id
),
enriched_subscriptions_2 AS (
    -- Attach customers attributes
    SELECT a.*, b.email
    FROM enriched_subscriptions a
    JOIN customers b
        ON a.customer_id = b.id
    WHERE b.segment = 'Corporate'
),
enriched_subscriptions_3 AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.start_date,
        s.monthly_fee,
        s.status,
        s.monthly_fee_avg_6,
        s.signup_date,
        s.country,
        s.city,
        s.email,
        c.city AS customer_city,
        c.signup_date AS customer_signup_date,
        c.credit_limit
    FROM enriched_subscriptions_2 s
    LEFT JOIN customers c
        ON s.customer_id = c.id
)
SELECT
    s.id,
    s.customer_id,
    s.start_date,
    s.status,
    s.monthly_fee_avg_6,
    s.signup_date,
    s.city,
    s.email,
    s.customer_city,
    (SELECT MIN(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS min_subscription_monthly_fee
FROM enriched_subscriptions_3 s
