WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        LEAST(unit_price, quantity) AS least_unit_price_quantity,
        quantity * unit_price AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
),
joined AS (
    -- Attach products attributes
    SELECT
        s.*,
        d.launch_date,
        d.stock_qty
    FROM filtered s
    LEFT JOIN products d
        ON s.product_id <=> d.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        launch_date,
        stock_qty,
        LEAD(line_total) OVER (PARTITION BY product_id ORDER BY least_unit_price_quantity, id) AS next_line_total,
        LEAD(unit_price) OVER (PARTITION BY product_id ORDER BY least_unit_price_quantity, id) AS next_unit_price,
        LAG(unit_price) OVER (PARTITION BY product_id ORDER BY least_unit_price_quantity, id) AS prev_unit_price,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY least_unit_price_quantity, id) AS event_seq
    FROM joined
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(stock_qty, 2, 0) OVER (PARTITION BY product_id ORDER BY event_seq, id) AS prev_stock_qty,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY unit_price DESC NULLS LAST) AS unit_price_dense_rank,
        SUM(stock_qty) OVER (PARTITION BY product_id ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_stock_qty
    FROM scored
)
SELECT
    s.id,
    s.product_id,
    s.quantity,
    s.launch_date,
    s.stock_qty,
    s.next_line_total,
    s.next_unit_price,
    s.prev_unit_price,
    s.prev_stock_qty,
    (SELECT AVG(t2.units) FROM store_sales t2 WHERE t2.product_id = s.product_id) AS avg_store_sale_units
FROM metrics s
ORDER BY launch_date
