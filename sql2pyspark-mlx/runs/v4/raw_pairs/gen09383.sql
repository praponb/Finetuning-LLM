WITH cleaned AS (
    -- Basic cleansing and defaults
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
        LENGTH(product_name) AS product_name_length
    FROM products
    WHERE launch_date > DATE '2023-06-07'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT category
    FROM cleaned
    EXCEPT
    SELECT category
    FROM products
    WHERE supplier_id IS NOT NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN remaining_keys k ON s.category = k.category
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        e.*,
        d.rating,
        d.country,
        d.supplier_name
    FROM reconciled e
    LEFT JOIN suppliers d
        ON e.supplier_id = d.id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        category,
        brand,
        stock_qty,
        launch_date,
        tags,
        country,
        supplier_name,
        ROUND(price * 0.85, 2) AS adjusted_price
    FROM enriched
    WHERE launch_date > DATE '2023-05-07'
      AND supplier_id IS NOT NULL
)
-- Multi-level aggregation
SELECT
    CASE WHEN stock_qty >= 420 THEN 'HIGH' ELSE 'LOW' END AS stock_qty_tier,
    BIT_AND(stock_qty) AS stock_qty_bit_and,
    COUNT(*) AS row_count
FROM filtered
GROUP BY 1
HAVING row_count > 1
ORDER BY stock_qty_bit_and DESC NULLS LAST;
