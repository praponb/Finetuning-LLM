WITH flattened AS (
    SELECT
        id,
        customer_id,
        element
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS element
)
SELECT
    customer_id,
    SIZE(COLLECT_SET(element)) AS num_elements,
    COUNT_IF(element = 'SPRING10') AS spring10_events,
    SIZE(COLLECT_SET(element)) AS num_elements_2
FROM flattened
GROUP BY customer_id
HAVING SIZE(COLLECT_SET(element)) > 1
ORDER BY customer_id DESC;
