WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.product_id IN (9, 13, 33, 36)
        )
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, category
    FROM eligible
    INTERSECT
    SELECT id, category
    FROM products
    WHERE cost > 260
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    JOIN remaining_keys k ON s.id = k.id AND s.category = k.category
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(price / SUM(price) OVER (PARTITION BY category), 4) AS price_share,
        CUME_DIST() OVER (ORDER BY price) AS price_cume_dist,
        RANK() OVER (PARTITION BY supplier_id ORDER BY stock_qty DESC) AS stock_qty_rank
    FROM kept_rows
),
eligible_products AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.product_id IS NULL
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (ORDER BY price DESC) AS price_rank,
        ROUND(stock_qty_rank / SUM(stock_qty_rank) OVER (PARTITION BY category), 4) AS stock_qty_rank_share,
        CORR(stock_qty, price) OVER (PARTITION BY brand) AS corr_stock_qty_price
    FROM eligible_products
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        category,
        brand,
        corr_stock_qty_price
    FROM metrics
) src
PIVOT (
    SUM(corr_stock_qty_price)
    FOR brand IN ('Globex', 'Stark')
);
