WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        supplier_id,
        CASE WHEN price >= 920 THEN 'High' WHEN price >= 540 THEN 'Medium' ELSE 'Low' END price_level,
        COALESCE(brand, 'UNKNOWN') resolved_brand
    FROM products
    WHERE launch_date >= DATE '2023-09-16'
      AND category = 'Grocery'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY cost DESC, id) seq_no,
        CORR(stock_qty, price) OVER (PARTITION BY supplier_id) corr_stock_qty_price
    FROM cleaned
),
kept_keys AS (
    -- Set difference on keys
    SELECT supplier_id
    FROM with_windows
    EXCEPT
    SELECT supplier_id
    FROM products
    WHERE LOWER(TRIM(product_name)) = 'noise-cancel headphones'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.supplier_id = k.supplier_id
),
grouped AS (
    -- Grouped metrics
    SELECT
        seq_no,
        CASE WHEN price >= 1160 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_tier,
        APPROX_PERCENTILE(stock_qty, ARRAY(0.25, 0.5, 0.75)) AS stock_qty_quartiles,
        COUNT(CASE WHEN brand = 'Umbrella' THEN 1 END) AS umbrella_count,
        COUNT(CASE WHEN brand = 'Acme' THEN 1 END) AS acme_count,
        DENSE_RANK() OVER (PARTITION BY seq_no ORDER BY COUNT(CASE WHEN brand = 'Acme' THEN 1 END) DESC) AS acme_count_dense_rank
    FROM kept_rows
    GROUP BY seq_no, CASE WHEN price >= 1160 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    seq_no,
    price_tier,
    acme_count
FROM grouped;
