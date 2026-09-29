WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS key, value
)
-- Ranking and gap features
SELECT
    *,
    LAST_VALUE(key, TRUE) OVER (PARTITION BY key ORDER BY created_at, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_key,
    FIRST_VALUE(key, TRUE) OVER (PARTITION BY key ORDER BY created_at, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_key,
    LAST_VALUE(key, TRUE) OVER (PARTITION BY customer_id, key ORDER BY created_at, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_key_2,
    FIRST_VALUE(key, TRUE) OVER (PARTITION BY key, customer_id ORDER BY created_at, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_key_2,
    ROW_NUMBER() OVER (PARTITION BY key ORDER BY created_at, id, key) event_seq
FROM exploded
