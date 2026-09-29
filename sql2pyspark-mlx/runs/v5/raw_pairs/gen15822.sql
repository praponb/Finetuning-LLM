WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS key, v
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        coupon_codes,
        key,
        UPPER(key) key_upper,
        COALESCE(key, 'NONE') key_filled
    FROM lines
    WHERE key IS DISTINCT FROM 'utm_source'
      AND customer_id IN (8, 15, 25, 27)
),
with_dims AS (
    -- Attach customers attributes
    SELECT src.*, d.country
    FROM valid src
    LEFT JOIN customers d
        ON src.customer_id <=> d.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.*,
        c.credit_limit,
        c.email
    FROM with_dims e
    LEFT JOIN customers c
        ON e.customer_id = c.id
),
enriched_carts AS (
    -- Attach customers attributes
    SELECT
        a.customer_id,
        a.key_upper,
        a.key_filled,
        a.credit_limit,
        a.email,
        p.credit_limit AS customer_credit_limit,
        p.country AS customer_country
    FROM enriched a
    LEFT JOIN customers p
        ON a.customer_id = p.id
)
SELECT
    s.customer_id,
    s.key_upper,
    s.credit_limit,
    s.email,
    s.customer_credit_limit,
    s.customer_country,
    CASE WHEN customer_country RLIKE '^(DE|UK)$' THEN 'PRIORITY' WHEN customer_country RLIKE '^(TH|US)$' THEN 'GROUP_B' ELSE 'CORE' END AS customer_country_zone,
    (SELECT SUM(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS sum_subscription_monthly_fee
FROM enriched_carts s
