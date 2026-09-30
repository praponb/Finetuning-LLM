WITH exploded AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS key, attr_value
),
flattened AS (
    SELECT
        id,
        customer_id,
        key,
        attr_value,
        code
    FROM exploded
    LATERAL VIEW EXPLODE(coupon_codes) e AS code
),
enriched AS (
    SELECT
        e.*,
        p.country,
        p.email
    FROM flattened e
    LEFT JOIN customers p
        ON e.customer_id = p.id
    WHERE p.segment IS NOT NULL
),
kept_keys AS (
    SELECT id, customer_id
    FROM enriched
    EXCEPT ALL
    SELECT id, customer_id
    FROM carts
    WHERE customer_id IS NULL
),
verified AS (
    SELECT s.*
    FROM enriched s
    JOIN kept_keys k ON s.id = k.id AND s.customer_id = k.customer_id
)
SELECT
    customer_id,
    SIZE(COLLECT_SET(code)) AS num_codes,
    COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count,
    COUNT(DISTINCT id) AS distinct_ids,
    SORT_ARRAY(COLLECT_SET(code)) AS code_list
FROM verified
GROUP BY customer_id;
