WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_name,
        brand,
        price,
        stock_qty,
        launch_date,
        LAG(cost) OVER (PARTITION BY supplier_id ORDER BY price, id) AS prev_cost,
        LAG(price) OVER (PARTITION BY supplier_id ORDER BY price, id) AS prev_price,
        ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY price, id) AS event_seq,
        FIRST_VALUE(price, TRUE) OVER (PARTITION BY brand, supplier_id ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_price
    FROM products
    WHERE supplier_id = 37
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        brand,
        price,
        stock_qty,
        prev_price,
        event_seq,
        CASE WHEN first_known_price >= 980 THEN 'High' WHEN first_known_price >= 520 THEN 'Medium' ELSE 'Low' END AS first_known_price_band,
        COALESCE(brand, 'UNKNOWN') AS brand_filled
    FROM metrics
    WHERE launch_date >= TO_DATE('2023-02-21')
      AND brand IS NOT NULL
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.quantity,
        top1.inventory_last_restock
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.quantity, c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        price,
        stock_qty,
        event_seq,
        first_known_price_band,
        inventory_last_restock,
        LEAD(stock_qty) OVER w_hist AS next_stock_qty,
        LAG(event_seq) OVER w_hist AS prev_event_seq,
        LEAD(event_seq) OVER w_hist AS next_event_seq,
        ROW_NUMBER() OVER w_hist AS row_seq
    FROM with_latest
    WINDOW w_hist AS (PARTITION BY brand ORDER BY price, id)
),
metrics_products AS (
    -- Ranking and gap features
    SELECT
        id,
        stock_qty,
        first_known_price_band,
        inventory_last_restock,
        next_event_seq,
        MIN(stock_qty) OVER (ORDER BY inventory_last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_stock_qty,
        AVG(stock_qty) OVER (ORDER BY inventory_last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_stock_qty,
        MAX(stock_qty) OVER (ORDER BY inventory_last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_stock_qty
    FROM with_windows
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        inventory_last_restock,
        FIRST_VALUE(running_max_stock_qty, TRUE) OVER (ORDER BY running_min_stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_max_stock_qty,
        LAST_VALUE(running_min_stock_qty, TRUE) OVER (ORDER BY next_event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_min_stock_qty,
        NTILE(5) OVER (ORDER BY running_max_stock_qty DESC, id) AS running_max_stock_qty_bucket,
        LAST_VALUE(next_event_seq) IGNORE NULLS OVER (ORDER BY next_event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_event_seq
    FROM metrics_products
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.product_id = s.id
              AND ch.store_id IN (2, 3, 6)
        )
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', inventory_last_restock) AS week_start,
    APPROX_PERCENTILE(running_max_stock_qty_bucket, 0.5) AS running_max_stock_qty_bucket_p50,
    ROUND(AVG(last_known_next_event_seq), 2) AS avg_last_known_next_event_seq,
    APPROX_PERCENTILE(last_known_next_event_seq, 0.9) AS last_known_next_event_seq_p90,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT id) AS distinct_ids
FROM eligible
GROUP BY DATE_TRUNC('WEEK', inventory_last_restock)
