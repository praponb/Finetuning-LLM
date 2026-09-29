WITH flattened AS (
    SELECT
        id,
        customer_id,
        attr_key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS attr_key, attr_value
)
SELECT
    s.id,
    s.customer_id,
    s.attr_key,
    CASE WHEN attr_key RLIKE '^(gift|device)$' THEN 'GROUP_B' WHEN attr_key RLIKE '^(utm_source|weight_kg)$' THEN 'GROUP_A' ELSE 'CORE' END AS attr_key_zone,
    (SELECT SUM(t2.amount) FROM orders t2 WHERE t2.customer_id = s.customer_id) AS sum_order_amount
FROM flattened s
ORDER BY customer_id DESC NULLS FIRST, sum_order_amount
