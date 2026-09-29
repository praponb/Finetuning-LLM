WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        attr_key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS attr_key, v
)
SELECT
    id,
    customer_id,
    created_at,
    attr_key
FROM flattened
ORDER BY created_at DESC, id DESC
