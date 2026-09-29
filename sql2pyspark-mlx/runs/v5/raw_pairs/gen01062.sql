WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        product_name,
        category,
        brand,
        launch_date,
        supplier_id,
        LEAD(price) OVER w_running next_price,
        LAG(cost) OVER w_running prev_cost,
        ROW_NUMBER() OVER w_running event_seq,
        ROUND(cost / SUM(cost) OVER (PARTITION BY brand), 4) cost_share
    FROM products
    WINDOW w_running AS (PARTITION BY brand ORDER BY stock_qty, id)
),
with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.order_item_unit_price
    FROM enriched s
    JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_last_products AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name,
        s.category,
        s.brand,
        s.event_seq,
        s.cost_share,
        top1.quantity
    FROM with_last s
    JOIN LATERAL (
            SELECT c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_last_products s
    WHERE EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.product_id = s.id
              AND x.product_id IN (3, 4, 8)
        )
)
-- Aggregate with conditional metrics
SELECT
    brand,
    ROUND(SUM(event_seq) / NULLIF(COUNT(CASE WHEN category = 'Electronics' THEN 1 END), 0), 2) AS event_seq_per_electronics,
    SIZE(COLLECT_SET(brand)) AS num_brands,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list
FROM kept
GROUP BY 1
ORDER BY brand DESC;
