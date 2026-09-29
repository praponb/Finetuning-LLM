WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS attr_key, v
),
lines_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        attr_key,
        v,
        value
    FROM lines
    LATERAL VIEW EXPLODE(coupon_codes) t AS value
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(attr_key) AS attr_key_upper,
        COALESCE(attr_key, 'NONE') AS attr_key_clean
    FROM lines_carts
    WHERE value IS NOT NULL
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.*,
        d.credit_limit,
        d.signup_date
    FROM valid e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        customer_id,
        attr_key_upper,
        credit_limit,
        AVG(credit_limit) OVER w_part AS customer_id_avg_credit_limit,
        MAX(credit_limit) OVER w_part_2 AS value_max_credit_limit,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY customer_id), 4) AS credit_limit_share,
        AVG(credit_limit) OVER (PARTITION BY value) AS value_avg_credit_limit,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY value), 4) AS credit_limit_share_2
    FROM with_dims
    WINDOW w_part AS (PARTITION BY customer_id), w_part_2 AS (PARTITION BY value, customer_id)
)
SELECT
    s.customer_id,
    s.attr_key_upper,
    s.credit_limit,
    s.customer_id_avg_credit_limit,
    s.credit_limit_share,
    s.value_avg_credit_limit,
    s.credit_limit_share_2,
    (SELECT MIN(t2.amount) FROM orders t2 WHERE t2.customer_id = s.customer_id) AS min_order_amount
FROM enriched s
ORDER BY credit_limit_share;
