WITH flattened AS (
    SELECT
        id,
        created_at,
        attr_key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS attr_key, attr_value
)
SELECT id, attr_key
FROM flattened
ORDER BY attr_key DESC, id
LIMIT 5
