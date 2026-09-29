WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        attr_key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS attr_key, value
)
-- Running and moving aggregates
SELECT
    id,
    created_at,
    attr_key,
    ROW_NUMBER() OVER (ORDER BY created_at ASC, id ASC, attr_key ASC) AS row_seq
FROM flattened;
