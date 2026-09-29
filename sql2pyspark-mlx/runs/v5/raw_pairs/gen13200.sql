WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        quantity * unit_price line_total,
        GREATEST(quantity, unit_price) greatest_quantity_unit_price
    FROM order_items
    WHERE product_id IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price, id) event_seq,
        LAG(unit_price, 1) OVER (PARTITION BY product_id ORDER BY greatest_quantity_unit_price, id) prev_unit_price,
        AVG(quantity) OVER (ORDER BY greatest_quantity_unit_price, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) quantity_avg_6,
        AVG(greatest_quantity_unit_price) OVER (PARTITION BY order_id, product_id ORDER BY greatest_quantity_unit_price, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) greatest_quantity_unit_price_avg_6_prior,
        MAX(quantity) OVER (PARTITION BY product_id) product_id_max_quantity
    FROM cleaned
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(greatest_quantity_unit_price, TRUE) OVER w_hist last_known_greatest_quantity_unit_price,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY event_seq DESC) event_seq_percent_rank
    FROM with_windows
    WINDOW w_hist AS (PARTITION BY order_id ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY greatest_quantity_unit_price_avg_6_prior DESC, id) = 1
)
-- Join a dimension and aggregate
SELECT
    p.channel,
    COUNT(*) AS num_rows,
    MAX(e.unit_price) AS max_unit_price,
    GROUPING_ID() AS grouping_level
FROM ranked e
LEFT JOIN orders p ON e.order_id = p.id
GROUP BY ROLLUP (p.channel);
