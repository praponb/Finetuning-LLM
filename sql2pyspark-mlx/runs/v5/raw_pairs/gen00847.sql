WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        element
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) t AS element
)
-- Grouped metrics
SELECT
    customer_id,
    element,
    SORT_ARRAY(COLLECT_SET(element)) AS element_list,
    COUNT(CASE WHEN element = 'SPRING10' THEN 1 END) AS spring10_count,
    SIZE(COLLECT_SET(element)) AS num_elements
FROM lines
GROUP BY customer_id, element
