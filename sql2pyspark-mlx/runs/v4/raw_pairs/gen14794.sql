WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT supplier_id
    FROM products
    INTERSECT
    SELECT supplier_id
    FROM products
    WHERE launch_date <= '2025-10-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM products s
    INNER JOIN remaining_keys k ON s.supplier_id = k.supplier_id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        PERCENT_RANK() OVER w_ordered AS stock_qty_percent_rank,
        ROUND(price / SUM(price) OVER (PARTITION BY supplier_id, brand), 4) AS price_share
    FROM verified
    WINDOW w_ordered AS (PARTITION BY category, supplier_id ORDER BY stock_qty DESC)
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT e.*, d.rating
    FROM with_windows e
    LEFT JOIN suppliers d
        ON e.supplier_id = d.id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS('/', brand, category) AS brand_category_key,
        price + price_share AS price_plus_price_share
    FROM with_dims
    WHERE launch_date > DATE '2023-12-10'
      AND supplier_id IS NOT NULL
),
daily AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('WEEK', launch_date) AS week_start,
        CASE WHEN stock_qty >= 420 THEN 'HIGH' ELSE 'LOW' END AS stock_qty_tier,
        CASE WHEN rating >= 4.6 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        APPROX_PERCENTILE(stock_qty, ARRAY(0.25, 0.5, 0.75)) AS stock_qty_quartiles,
        PERCENTILE_APPROX(rating, ARRAY(0.25, 0.5, 0.75)) AS rating_quartiles,
        GROUPING(DATE_TRUNC('WEEK', launch_date)) AS grouping_id
    FROM cleaned
    GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', launch_date), CASE WHEN stock_qty >= 420 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN rating >= 4.6 THEN 'HIGH' ELSE 'LOW' END), (DATE_TRUNC('WEEK', launch_date), CASE WHEN stock_qty >= 420 THEN 'HIGH' ELSE 'LOW' END))
)
SELECT
    stock_qty_tier,
    rating_tier,
    distinct_ids,
    stock_qty_quartiles,
    rating_quartiles,
    grouping_id
FROM daily
