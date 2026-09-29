WITH flattened AS (
    SELECT
        id,
        customer_id,
        k,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS k, value
)
SELECT
    s.id,
    s.customer_id,
    s.k,
    (SELECT MAX(x.amount) FROM orders x WHERE x.customer_id = s.customer_id) AS max_order_amount
FROM flattened s
