WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.*,
        c.age,
        c.city
    FROM flattened src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, WEEKOFYEAR(created_at) AS created_at_weekofyear
    FROM joined
    WHERE created_at > TO_TIMESTAMP('2025-03-02 13:00:00')
      AND city IS NOT NULL
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        coupon_codes,
        product_id,
        qty,
        age,
        city,
        created_at_weekofyear,
        ROUND(created_at_weekofyear, 1) AS created_at_weekofyear_rounded
    FROM filtered
    WHERE created_at > TO_DATE('2025-03-01')
      AND promo = 'WELCOME5'
)
-- Grouped metrics
SELECT
    CASE WHEN qty >= 5 THEN 'HIGH' ELSE 'LOW' END AS qty_tier,
    COUNT(*) AS cnt,
    COUNT(*) AS cnt_2,
    AVG(product_id) FILTER (WHERE city = 'Tokyo') AS tokyo_avg_product_id,
    ROUND(AVG(age), 2) AS avg_age,
    COUNT(DISTINCT city) AS distinct_cities
FROM base
GROUP BY 1
HAVING COUNT(DISTINCT city) > 1
