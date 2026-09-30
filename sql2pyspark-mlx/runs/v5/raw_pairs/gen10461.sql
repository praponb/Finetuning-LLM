WITH filtered AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        GREATEST(unit_price, quantity) greatest_unit_price_quantity,
        CAST(quantity AS DOUBLE) quantity_str
    FROM order_items
    WHERE product_id IS NOT NULL
),
ranked AS (
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity_str DESC, id) = 1
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY unit_price NULLS LAST) AS dedup_rank
        FROM ranked
    ) t
    WHERE dedup_rank = 1
),
cleaned AS (
    SELECT
        *,
        GREATEST(dedup_rank, unit_price) greatest_dedup_rank_unit_price,
        ROUND(greatest_unit_price_quantity, 2) greatest_unit_price_quantity_rounded
    FROM deduped
    WHERE product_id IS NOT NULL
      AND unit_price < 310
),
enriched AS (
    SELECT
        e.*,
        d.channel,
        d.discount,
        d.order_date
    FROM cleaned e
    JOIN orders d
        ON e.order_id = d.id
    WHERE d.amount > 1570
),
enriched_order_items AS (
    SELECT
        *,
        AVG(greatest_dedup_rank_unit_price) OVER (PARTITION BY product_id, channel ORDER BY order_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) greatest_dedup_rank_unit_price_avg_3_prior,
        SUM(unit_price) OVER (ORDER BY greatest_dedup_rank_unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_unit_price,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY product_id), 4) quantity_share
    FROM enriched
),
with_windows AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY quantity_share DESC NULLS FIRST) quantity_share_dense_rank,
        CORR(quantity, unit_price) OVER w_part corr_quantity_unit_price,
        NTILE(3) OVER w_running greatest_dedup_rank_unit_price_bucket,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY discount NULLS LAST) discount_cume_dist
    FROM enriched_order_items
    WINDOW w_part AS (PARTITION BY channel), w_running AS (PARTITION BY product_id ORDER BY greatest_dedup_rank_unit_price DESC, id)
)
SELECT
    product_id,
    channel,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    APPROX_PERCENTILE(greatest_unit_price_quantity, ARRAY(0.25, 0.5, 0.75)) AS greatest_unit_price_quantity_quartiles,
    PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY STDDEV_SAMP(quantity) DESC) AS quantity_stddev_percent_rank
FROM with_windows
GROUP BY product_id, channel
ORDER BY channel NULLS LAST;
