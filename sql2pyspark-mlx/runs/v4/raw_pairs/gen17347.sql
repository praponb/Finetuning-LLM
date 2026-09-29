WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        item.product_id,
        item.qty,
        item.price,
        item.promo
    FROM (
        SELECT
            id,
            customer_id,
            EXPLODE(items) AS item
        FROM carts
    ) li
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.city,
        p.name
    FROM exploded e
    LEFT JOIN customers p
        ON e.customer_id = p.id
),
enriched AS (
    -- Attach customers attributes
    SELECT a.*, d.age
    FROM with_dims a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        qty,
        price,
        promo,
        city,
        name,
        UPPER(TRIM(name)) AS name_normalized,
        COALESCE(city, 'NONE') AS city_filled
    FROM enriched
    WHERE customer_id IS NULL
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        qty,
        promo,
        city,
        name_normalized,
        city_filled,
        CASE WHEN promo IN ('BUNDLE', 'WELCOME5') THEN 'Tier 1' ELSE 'Other' END AS promo_group
    FROM valid
    WHERE price < 170
)
SELECT
    id,
    qty,
    promo,
    city,
    name_normalized,
    city_filled,
    CASE WHEN city RLIKE '^(Tokyo|Singapore)$' THEN 'CORE' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS city_zone
FROM filtered
