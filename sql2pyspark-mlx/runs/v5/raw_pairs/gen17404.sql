WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        t.id,
        t.customer_id,
        t.created_at,
        t.coupon_codes,
        line.qty,
        line.promo
    FROM carts t
    LATERAL VIEW EXPLODE(t.items) e AS line
),
exploded_carts AS (
    -- Unnest the array into one row per element
    SELECT
        customer_id,
        qty,
        EXPLODE_OUTER(coupon_codes) AS tag
    FROM exploded
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(tag)) AS tag_title,
        TRIM(tag) AS clean_tag,
        COALESCE(tag, 'OTHER') AS resolved_tag
    FROM exploded_carts
    WHERE qty >= 4
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        customer_id,
        qty,
        tag,
        tag_title,
        clean_tag,
        resolved_tag,
        COALESCE(tag, 'N/A') AS tag_filled,
        COALESCE(tag, 'N/A') AS tag_filled_2
    FROM cleaned
    WHERE tag IS NOT NULL
)
SELECT
    customer_id,
    tag_title,
    resolved_tag
FROM valid;
