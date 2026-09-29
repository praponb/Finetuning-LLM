WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        tag
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) e AS tag
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(tag, 'N/A') tag_filled
    FROM lines
    WHERE customer_id IS NOT NULL
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(tag) clean_tag,
        INITCAP(TRIM(tag)) tag_title,
        COALESCE(tag, 'NONE') resolved_tag
    FROM filtered
    WHERE tag <> 'VIP'
),
valid_carts AS (
    -- Basic cleansing and defaults
    SELECT
        customer_id,
        tag,
        clean_tag,
        LENGTH(tag) tag_length
    FROM valid
    WHERE customer_id IS NOT NULL
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        customer_id,
        tag,
        tag_length
    FROM valid_carts
) src
PIVOT (
    AVG(tag_length)
    FOR tag IN ('SPRING10', 'BUNDLE', 'VIP')
);
