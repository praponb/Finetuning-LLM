WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        MONTH(launch_date) launch_date_month,
        COALESCE(brand, 'OTHER') brand_clean
    FROM products
    WHERE brand IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY supplier_id ORDER BY price, id) price_bucket,
        MIN(stock_qty) OVER (PARTITION BY supplier_id) supplier_id_min_stock_qty,
        NTILE(5) OVER (PARTITION BY category ORDER BY stock_qty DESC, id) stock_qty_bucket,
        ROW_NUMBER() OVER w_hist seq_no
    FROM base
    WINDOW w_hist AS (PARTITION BY category ORDER BY launch_date, id)
),
enriched AS (
    -- Window metrics over several different partitions
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
        launch_date_month,
        brand_clean,
        price_bucket,
        supplier_id_min_stock_qty,
        stock_qty_bucket,
        seq_no,
        FIRST_VALUE(stock_qty, TRUE) OVER (PARTITION BY category, brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_stock_qty,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY launch_date, id) event_seq
    FROM with_windows
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        stock_qty,
        launch_date,
        tags,
        price_bucket,
        supplier_id_min_stock_qty,
        stock_qty_bucket,
        FLOOR(stock_qty) stock_qty_whole
    FROM enriched
    WHERE stock_qty_bucket BETWEEN 3 AND 6
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY category ORDER BY stock_qty DESC, id) stock_qty_bucket_2,
        ROW_NUMBER() OVER (ORDER BY launch_date, id) event_seq,
        MAX(stock_qty_bucket) OVER (PARTITION BY brand) brand_max_stock_qty_bucket
    FROM filtered
)
SELECT
    id,
    price,
    launch_date,
    supplier_id_min_stock_qty,
    stock_qty_bucket,
    stock_qty_whole,
    stock_qty_bucket_2,
    event_seq,
    brand_max_stock_qty_bucket
FROM scored;
