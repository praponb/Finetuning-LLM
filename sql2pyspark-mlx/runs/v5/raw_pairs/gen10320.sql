WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        price / NULLIF(stock_qty, 0) AS price_per_stock_qty,
        SUBSTRING(brand, 1, 3) AS brand_prefix
    FROM products
    WHERE supplier_id IS NOT NULL
      AND brand IS DISTINCT FROM 'Initech'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SORT_ARRAY(tags) AS sorted_tags,
        ROUND(cost / price, 2) AS margin
    FROM filtered
    WHERE supplier_id = 31
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT
        src.*,
        s.city,
        s.rating
    FROM cleaned src
    LEFT JOIN suppliers s
        ON src.supplier_id = s.id
    WHERE s.country IN ('UK', 'DE')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(rating) OVER (PARTITION BY city) AS city_avg_rating,
        LEAD(price_per_stock_qty, 2, 0) OVER (PARTITION BY city ORDER BY stock_qty, id) AS next_price_per_stock_qty,
        FIRST_VALUE(cost) IGNORE NULLS OVER (PARTITION BY supplier_id ORDER BY price_per_stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost
    FROM with_dims
)
SELECT
    id,
    product_name,
    price,
    cost,
    tags,
    price_per_stock_qty,
    brand_prefix,
    margin,
    city,
    CASE WHEN product_name RLIKE '^(Running Shoes|  Water Bottle)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS product_name_zone
FROM metrics
ORDER BY price DESC NULLS FIRST;
