WITH joined AS (
    -- Attach products attributes
    SELECT
        e.*,
        p.launch_date,
        p.brand,
        p.product_name
    FROM inventory e
    JOIN products p
        ON e.product_id = p.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        last_restock,
        launch_date,
        LEAD(quantity) OVER (PARTITION BY store_id, brand ORDER BY launch_date, id) AS next_quantity,
        ROW_NUMBER() OVER (PARTITION BY store_id, brand ORDER BY launch_date, id) AS row_seq,
        CUME_DIST() OVER (PARTITION BY store_id ORDER BY quantity DESC NULLS FIRST) AS quantity_cume_dist
    FROM joined
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_ordered AS seq_no,
        SUM(next_quantity) OVER (PARTITION BY product_id) AS product_id_sum_next_quantity,
        LAG(quantity_cume_dist, 2) OVER w_part AS prev_quantity_cume_dist,
        SUM(quantity_cume_dist) OVER (PARTITION BY product_id ORDER BY next_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity_cume_dist
    FROM metrics
    WINDOW w_ordered AS (PARTITION BY product_id ORDER BY quantity_cume_dist DESC, id), w_part AS (PARTITION BY product_id ORDER BY last_restock, id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY product_id_sum_next_quantity DESC, id) AS product_id_sum_next_quantity_bucket,
        NTILE(3) OVER (PARTITION BY product_id ORDER BY prev_quantity_cume_dist, id) AS prev_quantity_cume_dist_bucket
    FROM scored
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        next_quantity,
        row_seq,
        quantity_cume_dist,
        product_id_sum_next_quantity,
        prev_quantity_cume_dist,
        running_quantity_cume_dist
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock NULLS LAST, id) AS dedup_rank
        FROM with_windows
    ) x
    WHERE dedup_rank = 1
),
metrics_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(row_seq) OVER (ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_row_seq,
        ROW_NUMBER() OVER (ORDER BY quantity_cume_dist DESC, id) AS event_seq,
        FIRST_VALUE(product_id_sum_next_quantity, TRUE) OVER (ORDER BY prev_quantity_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_product_id_sum_next_quantity,
        ROW_NUMBER() OVER (ORDER BY quantity_cume_dist, id) AS event_seq_2
    FROM deduped
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, LEAST(product_id_sum_next_quantity, first_known_product_id_sum_next_quantity) AS least_product_id_sum_next_quantity_first_known_product_id_sum_next_quantity
    FROM metrics_inventory
    WHERE event_seq BETWEEN 5 AND 8
)
-- Multi-level aggregation
SELECT
    CASE WHEN row_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_tier,
    MAX(event_seq) AS max_event_seq,
    MAX(prev_quantity_cume_dist) AS max_prev_quantity_cume_dist,
    APPROX_PERCENTILE(least_product_id_sum_next_quantity_first_known_product_id_sum_next_quantity, 0.95) AS least_product_id_sum_next_quantity_first_known_product_id_sum_next_quantity_p95,
    PERCENTILE_APPROX(next_quantity, 0.9) AS next_quantity_p90,
    MAX(first_known_product_id_sum_next_quantity) AS max_first_known_product_id_sum_next_quantity,
    AVG(running_row_seq) AS avg_running_row_seq
FROM cleaned
GROUP BY CUBE (CASE WHEN row_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY next_quantity_p90 NULLS FIRST
