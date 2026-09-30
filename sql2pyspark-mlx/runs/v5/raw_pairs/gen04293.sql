WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(category) AS category_upper,
        COALESCE(brand, 'N/A') AS brand_clean
    FROM products
    WHERE supplier_id IS NOT NULL
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        a.id,
        a.product_name,
        a.brand,
        a.tags,
        a.supplier_id,
        p.rating
    FROM base a
    JOIN suppliers p
        ON a.supplier_id = p.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.revenue > 1800
        )
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        tags,
        supplier_id,
        rating,
        CASE WHEN rating >= 3.8 THEN 'High' WHEN rating >= 2.2 THEN 'Medium' ELSE 'Low' END AS rating_band
    FROM not_flagged
    WHERE supplier_id IS NOT NULL
      AND brand IS NOT DISTINCT FROM 'Initech'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY rating DESC NULLS FIRST, id) AS rn
    FROM valid
    QUALIFY rn = 1
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        src.id,
        src.product_name,
        src.supplier_id,
        src.rating_band,
        s.supplier_name,
        s.city
    FROM latest src
    LEFT JOIN suppliers s
        ON src.supplier_id = s.id
)
-- Aggregate with conditional metrics
SELECT
    city,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT city) AS distinct_cities,
    COUNT(*) AS row_count
FROM joined
GROUP BY city
ORDER BY distinct_cities
