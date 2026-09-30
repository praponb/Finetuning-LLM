WITH filtered AS (
    -- Filter the raw rows and derive helper columns
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
        CEIL(stock_qty) stock_qty_whole,
        COALESCE(brand, 'OTHER') brand_clean
    FROM products
    WHERE launch_date >= DATE '2023-09-05'
      AND cost > 460
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.quantity BETWEEN 3 AND 6
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.sale_date > '2024-04-01'
        )
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.category,
        s.price,
        s.cost,
        s.stock_qty,
        s.tags,
        s.stock_qty_whole,
        s.brand_clean,
        latest.units,
        latest.store_sale_sale_date
    FROM kept s
    JOIN LATERAL (
            SELECT c.units, c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        stock_qty,
        units,
        LEAD(price) OVER w_grp next_price,
        LAG(price) OVER w_grp prev_price,
        LEAD(stock_qty_whole) OVER w_grp next_stock_qty_whole,
        ROW_NUMBER() OVER w_grp seq_no
    FROM latest_match
    WINDOW w_grp AS (PARTITION BY category ORDER BY stock_qty, id)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        stock_qty,
        prev_price,
        next_stock_qty_whole,
        seq_no,
        LEAD(next_stock_qty_whole) OVER w_recent next_next_stock_qty_whole,
        LAG(next_price) OVER w_recent prev_next_price,
        ROW_NUMBER() OVER w_recent row_seq
    FROM with_windows
    WINDOW w_recent AS (ORDER BY seq_no, id)
),
with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.unit_price
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    next_stock_qty_whole,
    seq_no,
    next_next_stock_qty_whole,
    prev_next_price,
    row_seq,
    unit_price
FROM with_last
ORDER BY prev_next_price DESC
