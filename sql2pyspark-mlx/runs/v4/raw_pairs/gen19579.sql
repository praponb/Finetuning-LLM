WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        brand,
        price,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        UPPER(TRIM(product_name)) AS product_name_normalized
    FROM products
    WHERE brand IS NOT NULL
      AND brand = 'Initech'
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        a.*,
        s.rating,
        s.supplier_name,
        s.city
    FROM base a
    LEFT JOIN suppliers s
        ON a.supplier_id = s.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN price >= 1110 THEN 'High' WHEN price >= 540 THEN 'Medium' ELSE 'Low' END AS price_tier,
        UPPER(brand) AS brand_upper
    FROM joined
    WHERE launch_date >= DATE '2024-03-28'
      AND city IS NOT NULL
      AND brand IS NOT NULL
)
-- Grouped metrics
SELECT
    CASE WHEN rating >= 4.3 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_tier,
    supplier_id,
    COUNT(*) AS event_count,
    COUNT_IF(brand = 'Acme') AS acme_events,
    ROUND(SUM(rating) / NULLIF(COUNT(CASE WHEN brand = 'Stark' THEN 1 END), 0), 2) AS rating_per_stark
FROM cleaned
GROUP BY CASE WHEN rating >= 4.3 THEN 'PREMIUM' ELSE 'STANDARD' END, supplier_id;
