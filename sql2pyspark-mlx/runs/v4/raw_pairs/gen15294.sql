WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(product_name, 'N/A') AS product_name_filled,
        COALESCE(brand, 'OTHER') AS brand_filled
    FROM products
    WHERE launch_date > DATE '2024-03-16'
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT
        src.*,
        b.country,
        b.supplier_name,
        b.rating
    FROM valid src
    LEFT JOIN suppliers b
        ON src.supplier_id = b.id
),
matched_keys AS (
    -- Set intersection on keys
    SELECT supplier_id, id
    FROM with_dims
    INTERSECT
    SELECT supplier_id, id
    FROM products
    WHERE supplier_id IS NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN matched_keys k ON s.supplier_id = k.supplier_id AND s.id = k.id
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN stock_qty >= 335 THEN 'HIGH' ELSE 'LOW' END AS stock_qty_tier,
        COUNT(*) FILTER (WHERE brand = 'Acme') AS acme_rows,
        ROUND(SUM(rating) / NULLIF(COUNT(CASE WHEN country = 'SG' THEN 1 END), 0), 2) AS rating_per_sg,
        BIT_AND(stock_qty) AS stock_qty_bit_and,
        SIZE(COLLECT_SET(brand)) AS num_brands,
        BIT_AND(stock_qty) AS stock_qty_bit_and_2,
        STDDEV_SAMP(price) AS price_stddev
    FROM verified
    GROUP BY stock_qty_tier
    HAVING num_brands < 1
)
SELECT
    stock_qty_tier,
    acme_rows,
    rating_per_sg,
    num_brands,
    stock_qty_bit_and_2,
    price_stddev
FROM grouped;
