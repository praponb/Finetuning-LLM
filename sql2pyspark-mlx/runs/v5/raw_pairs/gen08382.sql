WITH base AS (
    -- Keep recent, valid rows
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
        FLOOR(price) price_whole,
        SORT_ARRAY(tags) sorted_tags
    FROM products
    WHERE launch_date > DATE '2023-09-02'
      AND brand = 'Initech'
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM base
    EXCEPT
    SELECT id
    FROM products
    WHERE ARRAY_CONTAINS(tags, 'new')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN matched_keys k ON s.id = k.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(stock_qty, 0) stock_qty_rounded,
        CAST(stock_qty AS STRING) stock_qty_str
    FROM reconciled
    WHERE launch_date >= DATE '2023-07-15'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        brand,
        price,
        cost,
        stock_qty_rounded,
        MIN(stock_qty_rounded) OVER (PARTITION BY category, supplier_id) category_min_stock_qty_rounded
    FROM valid
),
base_products AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        brand,
        price,
        cost,
        stock_qty_rounded,
        category_min_stock_qty_rounded,
        LOWER(brand) brand_lower,
        price / NULLIF(category_min_stock_qty_rounded, 0) price_per_category_min_stock_qty_rounded
    FROM enriched
    WHERE cost < 570
)
SELECT
    brand,
    cost,
    brand_lower,
    price_per_category_min_stock_qty_rounded
FROM base_products
