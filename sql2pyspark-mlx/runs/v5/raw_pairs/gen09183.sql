WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        li_item.qty,
        li_item.promo
    FROM (
        SELECT
            id,
            customer_id,
            created_at,
            coupon_codes,
            EXPLODE_OUTER(items) AS li_item
        FROM carts
    ) li
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        promo,
        EXPLODE(coupon_codes) AS element
    FROM exploded
),
joined AS (
    -- Attach customers attributes
    SELECT a.*, d.age
    FROM lines a
    LEFT JOIN customers d
        ON a.customer_id = d.id
    WHERE YEAR(d.signup_date) = 2024
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.created_at,
        e.promo,
        e.element,
        b.signup_date,
        b.email,
        b.name
    FROM joined e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        customer_id,
        created_at,
        promo,
        element,
        signup_date,
        email,
        name,
        LOWER(email) AS email_lower,
        COALESCE(promo, 'N/A') AS promo_clean
    FROM enriched
    WHERE created_at > DATE '2025-03-02'
)
SELECT
    customer_id,
    created_at,
    promo,
    signup_date,
    name,
    email_lower,
    promo_clean,
    CASE WHEN name RLIKE '^(Jack Taylor|Pim Chaiyaporn)$' THEN 'EXTENDED' WHEN name RLIKE '^(Alice Wong|bob smith)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS name_zone
FROM base
ORDER BY name_zone DESC;
