WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        EXPLODE(coupon_codes) AS tag
    FROM carts
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(tag) AS tag_length,
        INITCAP(TRIM(tag)) AS tag_title,
        COALESCE(tag, 'OTHER') AS tag_clean
    FROM exploded
    WHERE customer_id IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        customer_id,
        tag,
        tag_length,
        tag_title,
        tag_clean,
        CASE WHEN tag_length >= 86 THEN 'High' WHEN tag_length >= 33 THEN 'Medium' ELSE 'Low' END AS tag_length_level
    FROM valid
    WHERE tag IN ('SPRING10', 'WELCOME5')
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        tag,
        tag_length,
        tag_title,
        tag_clean,
        FLOOR(tag_length) AS tag_length_whole,
        COALESCE(tag, 'NONE') AS tag_clean_2
    FROM cleaned
    WHERE customer_id = 26
)
SELECT
    tag,
    tag_length,
    tag_title,
    tag_clean,
    tag_length_whole,
    tag_clean_2,
    CASE WHEN tag RLIKE '^(WELCOME5|VIP)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS tag_zone
FROM base
ORDER BY tag DESC NULLS FIRST;
