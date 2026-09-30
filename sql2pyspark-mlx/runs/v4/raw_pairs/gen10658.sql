WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS attr_key, attr_value
),
lines AS (
    SELECT
        id,
        created_at,
        attr_key,
        attr_value,
        code
    FROM exploded
    LATERAL VIEW OUTER EXPLODE(coupon_codes) x AS code
),
valid AS (
    SELECT
        *,
        UPPER(attr_key) AS attr_key_upper,
        COALESCE(attr_key, 'N/A') AS attr_key_filled
    FROM lines
    WHERE code = 'SPRING10'
),
valid_carts AS (
    SELECT
        id,
        attr_key,
        attr_value,
        code,
        attr_key_upper,
        attr_key_filled,
        COALESCE(code, 'Unknown') AS code_filled,
        COALESCE(attr_key, 'OTHER') AS attr_key_filled_2
    FROM valid
    WHERE created_at >= TIMESTAMP '2025-03-02 06:00:00'
      AND code IS NOT NULL
),
summary AS (
    SELECT
        attr_key,
        COUNT_IF(attr_key = 'weight_kg') AS weight_kg_events,
        COUNT(DISTINCT attr_key) AS distinct_attr_keys,
        COUNT(*) AS event_count,
        COUNT(CASE WHEN attr_key = 'gift' THEN 1 END) AS gift_count,
        SORT_ARRAY(COLLECT_SET(code)) AS code_list,
        COUNT(*) AS num_rows
    FROM valid_carts
    GROUP BY attr_key
)
SELECT
    attr_key,
    weight_kg_events,
    distinct_attr_keys,
    gift_count
FROM summary
ORDER BY weight_kg_events DESC;
