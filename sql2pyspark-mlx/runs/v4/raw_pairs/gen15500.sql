WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS k, value
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        created_at,
        k,
        value,
        CASE WHEN k = 'weight_kg' THEN 1 ELSE 0 END AS is_weight_kg
    FROM flattened
    WHERE created_at >= TO_TIMESTAMP('2025-03-01 21:00:00')
),
with_dims AS (
    -- Attach customers attributes
    SELECT src.*, c.credit_limit
    FROM valid src
    JOIN customers c
        ON src.customer_id = c.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(is_weight_kg) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS is_weight_kg_max_30m,
        MAX(is_weight_kg) OVER w_recent AS customer_id_max_is_weight_kg,
        COUNT(is_weight_kg) OVER (PARTITION BY k, customer_id) AS k_count_is_weight_kg
    FROM with_dims
    WINDOW w_recent AS (PARTITION BY customer_id)
)
SELECT
    s.id,
    s.customer_id,
    s.created_at,
    s.k,
    s.value,
    s.credit_limit,
    s.is_weight_kg_max_30m,
    s.customer_id_max_is_weight_kg,
    (SELECT MIN(t2.shipping_fee) FROM orders t2 WHERE t2.customer_id = s.customer_id) AS min_order_shipping_fee
FROM enriched s
