WITH cleaned AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        CAST(quantity AS DOUBLE) AS quantity_num
    FROM order_items
    WHERE quantity > 3
),
filtered AS (
    SELECT
        *,
        unit_price - quantity AS line_total,
        quantity * quantity_num AS quantity_times_quantity_num
    FROM cleaned
    WHERE product_id IS NOT NULL
      AND order_id IN (8, 30)
),
enriched AS (
    SELECT
        e.*,
        b.brand,
        b.product_name
    FROM filtered e
    INNER JOIN products b
        ON e.product_id = b.id
),
with_windows AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        quantity_num,
        line_total,
        quantity_times_quantity_num,
        brand,
        product_name,
        ROW_NUMBER() OVER w_running AS row_seq,
        SUM(quantity) OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity,
        CORR(unit_price, quantity) OVER w_recent AS corr_unit_price_quantity,
        MIN(quantity) OVER w_recent AS product_id_min_quantity
    FROM enriched
    WINDOW w_running AS (PARTITION BY brand ORDER BY unit_price DESC, id), w_recent AS (PARTITION BY product_id)
),
grouped AS (
    SELECT
        brand,
        ROUND(SUM(line_total) / NULLIF(COUNT(CASE WHEN brand = 'Stark' THEN 1 END), 0), 2) AS line_total_per_stark,
        SIZE(COLLECT_SET(brand)) AS num_brands,
        COUNT_IF(brand = 'Stark') AS stark_events,
        MAX(quantity_num) FILTER (WHERE brand = 'Umbrella') AS umbrella_max_quantity_num,
        PERCENTILE_APPROX(row_seq, 0.95) AS row_seq_p95
    FROM with_windows
    GROUP BY brand
    HAVING stark_events >= 1
)
SELECT
    brand,
    line_total_per_stark,
    num_brands,
    stark_events,
    umbrella_max_quantity_num,
    row_seq_p95
FROM grouped
ORDER BY umbrella_max_quantity_num DESC, brand DESC
LIMIT 20
