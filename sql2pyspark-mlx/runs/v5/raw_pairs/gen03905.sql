WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        attr_key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS attr_key, v
)
-- Grouped metrics
SELECT
    attr_key,
    COUNT(*) FILTER (WHERE attr_key = 'gift') AS gift_rows,
    COUNT(*) AS cnt,
    COUNT(CASE WHEN attr_key = 'weight_kg' THEN 1 END) AS weight_kg_count,
    SORT_ARRAY(COLLECT_SET(attr_key)) AS attr_key_list
FROM exploded
GROUP BY attr_key
ORDER BY cnt DESC NULLS LAST;
