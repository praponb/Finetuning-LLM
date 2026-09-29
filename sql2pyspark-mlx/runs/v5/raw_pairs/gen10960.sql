WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(quantity, 1) OVER w_grp prev_quantity,
        LAST_VALUE(quantity, TRUE) OVER (PARTITION BY store_id, product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_quantity,
        MIN(quantity) OVER (PARTITION BY product_id) product_id_min_quantity
    FROM inventory
    WHERE last_restock > '2023-06-19'
    WINDOW w_grp AS (ORDER BY quantity, id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(quantity) OVER w_recent running_quantity,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) event_seq
    FROM enriched
    WINDOW w_recent AS (PARTITION BY store_id, product_id ORDER BY product_id_min_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(event_seq) OVER (PARTITION BY store_id, product_id ORDER BY prev_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_event_seq,
        AVG(event_seq) OVER (PARTITION BY store_id, product_id ORDER BY prev_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_event_seq,
        MAX(event_seq) OVER (PARTITION BY store_id, product_id ORDER BY prev_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_event_seq
    FROM with_windows
),
enriched_inventory AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY prev_quantity, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) quantity_avg_5,
        STDDEV_SAMP(quantity) OVER (PARTITION BY store_id) store_id_stddev_samp_quantity,
        NTILE(5) OVER (PARTITION BY product_id ORDER BY event_seq, id) event_seq_bucket,
        LAST_VALUE(product_id_min_quantity) IGNORE NULLS OVER (PARTITION BY store_id, product_id ORDER BY prev_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_product_id_min_quantity
    FROM scored
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.last_restock,
    s.prev_quantity,
    s.last_known_quantity,
    s.product_id_min_quantity,
    s.running_quantity,
    s.event_seq,
    (SELECT AVG(x.units) FROM store_sales x WHERE x.product_id = s.product_id) AS avg_store_sale_units
FROM enriched_inventory s
ORDER BY store_id DESC NULLS FIRST, product_id;
