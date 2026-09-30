WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_name,
        brand,
        stock_qty,
        tags,
        supplier_id,
        FLOOR(price) AS price_whole,
        COALESCE(brand, 'DIRECT') AS brand_filled
    FROM products
    WHERE launch_date >= DATE '2023-05-18'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY stock_qty DESC NULLS LAST, id) AS rn
    FROM cleaned
    QUALIFY rn = 1
),
ranked_products AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand, supplier_id ORDER BY stock_qty NULLS LAST, id) AS dedup_rank
    FROM ranked
    QUALIFY dedup_rank = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked_products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.product_id = s.id
              AND x.store_id IN (1, 3, 4, 6)
        )
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY supplier_id, brand ORDER BY stock_qty NULLS LAST) AS stock_qty_percent_rank,
        FIRST_VALUE(stock_qty) IGNORE NULLS OVER w_recent AS first_known_stock_qty
    FROM not_flagged
    WINDOW w_recent AS (PARTITION BY brand ORDER BY price_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        supplier_id,
        brand,
        stock_qty_percent_rank
    FROM scored
) src
PIVOT (
    SUM(stock_qty_percent_rank)
    FOR brand IN ('Umbrella', 'Globex', 'Stark')
);
