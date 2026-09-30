WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        FLOOR(unit_price) AS unit_price_whole,
        GREATEST(quantity, unit_price) AS greatest_quantity_unit_price
    FROM order_items
    WHERE product_id IS NOT NULL
),
with_dims AS (
    -- Attach products attributes
    SELECT s.*, p.launch_date
    FROM cleaned s
    LEFT JOIN products p
        ON s.product_id = p.id
    WHERE UPPER(TRIM(p.product_name)) = 'LAPTOP PRO 14'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price_whole,
        launch_date,
        COALESCE(greatest_quantity_unit_price, 0) AS greatest_quantity_unit_price_filled
    FROM with_dims
    WHERE product_id IS NOT NULL
      AND product_id IS NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY greatest_quantity_unit_price_filled DESC NULLS LAST) AS greatest_quantity_unit_price_filled_cume_dist,
        NTILE(3) OVER w_running AS unit_price_whole_bucket,
        AVG(unit_price_whole) OVER (ORDER BY greatest_quantity_unit_price_filled, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS unit_price_whole_avg_3_prior,
        LEAD(unit_price_whole, 2) OVER w_running_2 AS next_unit_price_whole,
        SUM(quantity) OVER w_hist AS running_quantity
    FROM filtered
    WINDOW w_running AS (PARTITION BY product_id ORDER BY unit_price_whole, id), w_running_2 AS (ORDER BY greatest_quantity_unit_price_filled, id), w_hist AS (ORDER BY greatest_quantity_unit_price_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    s.id,
    s.order_id,
    s.product_id,
    s.quantity,
    s.unit_price_whole,
    s.greatest_quantity_unit_price_filled,
    s.unit_price_whole_bucket,
    (SELECT MAX(o2.unit_price) FROM order_items o2 WHERE o2.product_id = s.product_id) AS max_order_item_unit_price
FROM enriched s
ORDER BY order_id DESC;
