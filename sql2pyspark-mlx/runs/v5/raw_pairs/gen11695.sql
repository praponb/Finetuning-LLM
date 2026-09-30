WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        ROUND(quantity / unit_price, 2) AS line_total
    FROM order_items
    WHERE unit_price > 290
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        unit_price,
        LAG(quantity) OVER (PARTITION BY order_id ORDER BY unit_price, id) AS prev_quantity,
        LAG(line_total) OVER (PARTITION BY order_id ORDER BY unit_price, id) AS prev_line_total,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price, id) AS seq_no
    FROM cleaned
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price DESC, id) AS row_num
        FROM metrics
    ) t
    WHERE row_num = 1
),
enriched AS (
    -- Attach products attributes
    SELECT a.*, p.brand
    FROM deduped a
    INNER JOIN products p
        ON a.product_id = p.id
),
deduped_order_items AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY brand, product_id ORDER BY unit_price) AS latest_rank
    FROM enriched
    QUALIFY latest_rank = 1
),
joined AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.product_id,
        a.unit_price,
        a.prev_quantity,
        a.prev_line_total,
        a.row_num,
        a.latest_rank,
        b.stock_qty
    FROM deduped_order_items a
    LEFT JOIN products b
        ON a.product_id <=> b.id
),
joined_order_items AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.product_id,
        a.unit_price,
        a.prev_quantity,
        a.prev_line_total,
        a.row_num,
        a.latest_rank,
        b.launch_date,
        b.cost
    FROM joined a
    INNER JOIN products b
        ON a.product_id <=> b.id
    WHERE b.supplier_id IS NOT NULL
)
-- Join a dimension and aggregate
SELECT
    p.category,
    COUNT(*) AS rows_total,
    MIN(e.cost) AS min_cost,
    GROUPING_ID() AS level_id
FROM joined_order_items e
INNER JOIN products p ON e.product_id = p.id
GROUP BY CUBE (p.category);
