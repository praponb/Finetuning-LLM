WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM products
    QUALIFY ROW_NUMBER() OVER (PARTITION BY supplier_id, brand ORDER BY launch_date, id) = 1
),
with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name,
        s.category,
        s.price,
        s.cost,
        s.stock_qty,
        latest.order_item_quantity
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.product_id IN (4, 31)
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        price,
        cost,
        order_item_quantity,
        AVG(stock_qty) OVER (PARTITION BY category ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_stock_qty,
        COUNT(stock_qty) OVER (PARTITION BY category ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_stock_qty,
        MAX(stock_qty) OVER (PARTITION BY category ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_stock_qty,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY price, id) AS event_seq
    FROM eligible
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN running_count_stock_qty >= 17 THEN 'LARGE' ELSE 'SMALL' END AS running_count_stock_qty_tier,
        CASE WHEN running_avg_stock_qty >= 300 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_avg_stock_qty_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        APPROX_PERCENTILE(running_max_stock_qty, 0.5) AS running_max_stock_qty_p50,
        COUNT(DISTINCT id) AS distinct_ids_2,
        APPROX_PERCENTILE(running_max_stock_qty, ARRAY(0.25, 0.5, 0.75)) AS running_max_stock_qty_quartiles,
        ROUND(AVG(order_item_quantity), 2) AS avg_order_item_quantity
    FROM scored
    GROUP BY CUBE (CASE WHEN running_count_stock_qty >= 17 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN running_avg_stock_qty >= 300 THEN 'PREMIUM' ELSE 'STANDARD' END)
)
SELECT
    running_count_stock_qty_tier,
    running_avg_stock_qty_tier,
    distinct_ids,
    running_max_stock_qty_p50,
    distinct_ids_2,
    avg_order_item_quantity
FROM summary
ORDER BY distinct_ids_2 DESC NULLS LAST, running_count_stock_qty_tier;
