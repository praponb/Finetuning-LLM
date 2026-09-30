WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS key, v
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        key,
        EXPLODE(coupon_codes) AS value
    FROM exploded
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        value,
        CASE WHEN key IN ('device', 'gift') THEN 'Tier 1' ELSE 'Other' END AS key_group,
        CONCAT(key, ' ', value) AS key_value,
        COALESCE(key, 'NONE') AS key_filled
    FROM flattened
    WHERE created_at >= '2023-07-01'
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.customer_id,
        a.key_group,
        a.key_value,
        p.country,
        p.segment,
        p.name
    FROM cleaned a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
enriched_carts AS (
    -- Attach customers attributes
    SELECT
        a.customer_id,
        a.key_group,
        a.key_value,
        p.segment AS customer_segment,
        p.email
    FROM enriched a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
summary AS (
    -- Grouped metrics
    SELECT
        customer_id,
        SIZE(COLLECT_SET(customer_segment)) AS num_customer_segments,
        SIZE(COLLECT_SET(customer_segment)) AS num_customer_segments_2,
        COUNT(CASE WHEN customer_segment = 'Consumer' THEN 1 END) AS consumer_count,
        COUNT(CASE WHEN customer_segment = 'SMB' THEN 1 END) AS smb_count
    FROM enriched_carts
    GROUP BY customer_id
)
SELECT
    s.customer_id,
    s.num_customer_segments,
    s.smb_count,
    (SELECT AVG(o2.shipping_fee) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS avg_order_shipping_fee
FROM summary s
ORDER BY customer_id, num_customer_segments
LIMIT 5;
