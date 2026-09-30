WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT id, EXPLODE_OUTER(coupon_codes) AS tag
    FROM carts
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(tag) AS tag_lower
    FROM flattened
    WHERE tag IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM filtered
    EXCEPT
    SELECT id
    FROM carts
    WHERE TO_DATE(created_at) >= '2024-11-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.id = k.id
)
SELECT
    id,
    tag,
    tag_lower,
    CASE WHEN tag RLIKE '^(SPRING10|VIP)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS tag_zone
FROM kept_rows;
