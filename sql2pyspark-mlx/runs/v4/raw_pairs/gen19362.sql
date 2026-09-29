WITH matched_keys AS (
    -- Set difference on keys
    SELECT id, supplier_id
    FROM products
    EXCEPT
    SELECT id, supplier_id
    FROM products
    WHERE price <= 1030
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM products s
    INNER JOIN matched_keys k ON s.id = k.id AND s.supplier_id = k.supplier_id
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('-', product_name, brand) AS product_name_brand_key,
        COALESCE(brand, 'OTHER') AS resolved_brand
    FROM reconciled
    WHERE launch_date > TO_DATE('2024-01-21')
      AND supplier_id IN (20, 23, 26)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY brand ORDER BY stock_qty) AS stock_qty_dense_rank,
        STDDEV_SAMP(price) OVER (PARTITION BY brand) AS brand_stddev_samp_price,
        AVG(stock_qty) OVER (PARTITION BY brand) AS brand_avg_stock_qty
    FROM base
)
-- Window metrics over several different partitions
SELECT
    *,
    STDDEV_SAMP(price) OVER (PARTITION BY category) AS category_stddev_samp_price,
    PERCENT_RANK() OVER (ORDER BY price NULLS FIRST) AS price_percent_rank,
    DENSE_RANK() OVER (PARTITION BY supplier_id ORDER BY cost NULLS LAST) AS cost_dense_rank
FROM metrics;
