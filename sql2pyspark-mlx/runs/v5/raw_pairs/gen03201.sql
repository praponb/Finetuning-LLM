WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN unit_price >= 350 THEN 'High' WHEN unit_price >= 170 THEN 'Medium' ELSE 'Low' END AS unit_price_tier,
        unit_price * quantity AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND quantity BETWEEN 4 AND 7
),
enriched AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.order_id,
        s.product_id,
        s.quantity,
        s.line_total,
        p.stock_qty,
        p.cost
    FROM cleaned s
    LEFT JOIN products p
        ON s.product_id <=> p.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        order_id,
        quantity,
        stock_qty,
        cost
    FROM enriched
    QUALIFY RANK() OVER (PARTITION BY order_id, product_id ORDER BY cost DESC NULLS FIRST) = 1
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        quantity,
        CASE WHEN stock_qty >= 446 THEN 'High' WHEN stock_qty >= 190 THEN 'Medium' ELSE 'Low' END AS stock_qty_tier,
        FLOOR(stock_qty) AS stock_qty_whole
    FROM ranked
    WHERE order_id = 36
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(quantity * 1.07, 2) AS quantity_adjusted,
        stock_qty_whole + quantity AS stock_qty_whole_plus_quantity
    FROM valid
    WHERE stock_qty_whole != 26
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        quantity,
        LEAD(stock_qty_whole, 1) OVER w_part AS next_stock_qty_whole,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity_adjusted DESC, id) AS seq_no,
        AVG(quantity_adjusted) OVER w_grp AS quantity_adjusted_avg_2,
        LAG(quantity_adjusted, 1) OVER w_part AS prev_quantity_adjusted
    FROM filtered
    WHERE stock_qty_whole_plus_quantity BETWEEN 58 AND 75
    WINDOW w_part AS (ORDER BY stock_qty_whole, id), w_grp AS (PARTITION BY order_id ORDER BY stock_qty_whole_plus_quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    seq_no,
    COUNT(DISTINCT id) AS distinct_ids,
    MIN(next_stock_qty_whole) AS min_next_stock_qty_whole,
    APPROX_PERCENTILE(prev_quantity_adjusted, 0.95) AS prev_quantity_adjusted_p95,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(quantity_adjusted_avg_2, ARRAY(0.25, 0.5, 0.75)) AS quantity_adjusted_avg_2_quartiles
FROM scored
GROUP BY CUBE (seq_no)
ORDER BY prev_quantity_adjusted_p95 NULLS LAST
