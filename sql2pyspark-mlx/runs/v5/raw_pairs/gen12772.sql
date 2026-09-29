WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, CEIL(quantity) quantity_whole
    FROM order_items
    WHERE product_id IS NOT NULL
),
with_dims AS (
    -- Attach products attributes
    SELECT e.*, p.cost
    FROM valid e
    LEFT JOIN products p
        ON e.product_id = p.id
    WHERE p.launch_date BETWEEN '2024-07-06' AND '2024-08-18'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CAST(quantity_whole AS STRING) quantity_whole_str
    FROM with_dims
    WHERE cost > 260
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, product_id
    FROM cleaned
    EXCEPT ALL
    SELECT id, product_id
    FROM order_items
    WHERE order_id IN (16, 22, 38)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.id = k.id AND s.product_id = k.product_id
),
valid_order_items AS (
    -- Keep recent, valid rows
    SELECT *, quantity - unit_price line_total
    FROM verified
    WHERE cost >= 460
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        quantity_whole,
        cost,
        quantity_whole_str,
        line_total,
        ROUND(quantity, 2) quantity_rounded,
        ROUND(line_total * 100, 2) adjusted_line_total
    FROM valid_order_items
    WHERE product_id IN (27, 32)
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        adjusted_line_total * quantity_whole adjusted_line_total_times_quantity_whole,
        CASE WHEN quantity_rounded >= 77 THEN 'High' WHEN quantity_rounded >= 38 THEN 'Medium' ELSE 'Low' END quantity_rounded_band
    FROM filtered
    WHERE product_id IS NOT NULL
)
SELECT
    s.id,
    s.order_id,
    s.product_id,
    s.unit_price,
    s.quantity_whole,
    s.cost,
    s.quantity_whole_str,
    s.quantity_rounded,
    s.adjusted_line_total,
    (SELECT SUM(x.revenue) FROM store_sales x WHERE x.product_id = s.product_id) AS sum_store_sale_revenue
FROM base s;
