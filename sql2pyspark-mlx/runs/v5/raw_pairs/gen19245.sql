WITH cleaned AS (
    SELECT *, CAST(quantity AS STRING) AS quantity_str
    FROM order_items
    WHERE product_id IS NOT NULL
),
latest AS (
    SELECT
        id,
        order_id,
        quantity
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price DESC NULLS FIRST, id) AS row_num
        FROM cleaned
    ) x
    WHERE row_num = 1
),
filtered AS (
    SELECT *, CASE WHEN quantity >= 7 THEN 'High' WHEN quantity >= 5 THEN 'Medium' ELSE 'Low' END AS quantity_band
    FROM latest
    WHERE quantity > 6
),
filtered_order_items AS (
    SELECT *, CAST(quantity AS STRING) AS quantity_num
    FROM filtered
    WHERE order_id IN (3, 20, 24, 27)
),
scored AS (
    SELECT
        id,
        order_id,
        quantity,
        quantity_band,
        quantity_num,
        AVG(quantity) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS quantity_avg_5_prior,
        NTILE(5) OVER (PARTITION BY order_id ORDER BY quantity, id) AS quantity_bucket,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY quantity) AS quantity_percent_rank,
        SUM(quantity) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity,
        NTILE(10) OVER (ORDER BY quantity, id) AS quantity_bucket_2
    FROM filtered_order_items
    WHERE order_id IN (13, 20, 37, 40)
)
SELECT
    o.channel,
    COUNT(*) AS rows_total,
    AVG(e.quantity_avg_5_prior) AS avg_quantity_avg_5_prior,
    GROUPING_ID() AS level_id
FROM scored e
INNER JOIN orders o ON e.order_id = o.id
GROUP BY ROLLUP (o.channel)
ORDER BY rows_total NULLS FIRST;
