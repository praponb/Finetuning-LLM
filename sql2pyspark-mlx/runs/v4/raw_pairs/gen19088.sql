WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.product_id = s.id
              AND ch.units > 15
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.unit_price BETWEEN 120 AND 260
        )
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM eligible
    QUALIFY ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY launch_date DESC NULLS FIRST, id ASC) = 1
),
with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.quantity,
        top1.unit_price
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.quantity, c.unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Ranking and gap features
SELECT
    *,
    LAG(unit_price, 1) OVER (ORDER BY quantity ASC, id ASC) AS prev_unit_price,
    CORR(stock_qty, price) OVER (PARTITION BY category, brand) AS corr_stock_qty_price,
    NTILE(10) OVER (PARTITION BY supplier_id, brand ORDER BY price ASC, id ASC) AS price_bucket,
    LAG(stock_qty, 1, 0) OVER (PARTITION BY category ORDER BY cost ASC, id ASC) AS prev_stock_qty,
    NTILE(10) OVER w_recent AS unit_price_bucket
FROM with_latest
WINDOW w_recent AS (PARTITION BY category ORDER BY unit_price ASC, id ASC);
