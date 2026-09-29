WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        tag
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) t AS tag
)
-- Multi-level aggregation
SELECT
    customer_id,
    tag,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT tag) AS distinct_tags,
    COUNT(CASE WHEN tag = 'VIP' THEN 1 END) AS vip_count,
    COUNT(CASE WHEN tag = 'VIP' THEN 1 END) AS vip_count_2,
    SORT_ARRAY(COLLECT_SET(tag)) AS tag_list,
    SORT_ARRAY(COLLECT_SET(tag)) AS tag_list_2
FROM lines
GROUP BY customer_id, tag
ORDER BY vip_count DESC;
