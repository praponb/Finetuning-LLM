WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(brand, 1, 4) AS brand_prefix,
        COALESCE(brand, 'UNKNOWN') AS brand_filled
    FROM products
    WHERE brand IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(stock_qty) OVER (PARTITION BY brand, supplier_id ORDER BY launch_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_stock_qty,
        MAX(stock_qty) OVER (PARTITION BY brand, supplier_id ORDER BY launch_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_stock_qty,
        AVG(stock_qty) OVER (PARTITION BY brand, supplier_id ORDER BY launch_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_stock_qty,
        NTILE(10) OVER (PARTITION BY supplier_id, brand ORDER BY price ASC, id ASC) AS price_bucket
    FROM filtered
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(price) OVER (PARTITION BY supplier_id) AS supplier_id_sum_price,
        NTILE(10) OVER (PARTITION BY brand, category ORDER BY price ASC, id ASC) AS price_bucket_2
    FROM metrics
    WHERE ARRAY_CONTAINS(tags, 'clearance')
),
kept_keys AS (
    -- Set intersection on keys
    SELECT category
    FROM enriched
    INTERSECT
    SELECT category
    FROM products
    WHERE product_name IS NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.category = k.category
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        brand,
        CASE WHEN last7_avg_stock_qty >= 350 THEN 'HIGH' ELSE 'LOW' END AS last7_avg_stock_qty_tier,
        COUNT(*) FILTER (WHERE category = 'Sports') AS sports_rows,
        SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
        SIZE(COLLECT_SET(category)) AS num_categories,
        BIT_OR(stock_qty) AS stock_qty_bit_or,
        GROUPING(brand) AS grouping_id
    FROM verified
    GROUP BY GROUPING SETS ((brand, CASE WHEN last7_avg_stock_qty >= 350 THEN 'HIGH' ELSE 'LOW' END), (brand))
    HAVING COUNT(*) FILTER (WHERE category = 'Sports') > 1
)
SELECT
    brand,
    last7_avg_stock_qty_tier,
    sports_rows,
    brand_list,
    num_categories,
    stock_qty_bit_or,
    grouping_id,
    CASE WHEN brand RLIKE '^(Umbrella|Initech)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS brand_zone
FROM stats
ORDER BY brand_zone DESC
