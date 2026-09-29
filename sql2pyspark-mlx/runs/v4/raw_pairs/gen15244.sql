WITH flattened AS (
    SELECT
        id,
        customer_id,
        EXPLODE_OUTER(coupon_codes) AS element
    FROM carts
)
SELECT
    customer_id,
    SIZE(COLLECT_SET(element)) AS num_elements,
    COUNT(CASE WHEN element = 'VIP' THEN 1 END) AS vip_count,
    COUNT(DISTINCT id) AS distinct_ids,
    SORT_ARRAY(COLLECT_SET(element)) AS element_list
FROM flattened
GROUP BY customer_id
