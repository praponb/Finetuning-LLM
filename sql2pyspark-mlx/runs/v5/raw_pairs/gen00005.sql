WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        unit_price,
        LAG(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id) AS prev_quantity,
        LEAD(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id) AS next_quantity,
        LAG(unit_price) OVER (PARTITION BY product_id ORDER BY quantity, id) AS prev_unit_price,
        LEAD(unit_price) OVER (PARTITION BY product_id ORDER BY quantity, id) AS next_unit_price,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) AS row_seq
    FROM order_items
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        next_quantity,
        prev_unit_price,
        LAG(row_seq) OVER w_grp AS prev_row_seq,
        LEAD(row_seq) OVER w_grp AS next_row_seq,
        LAG(unit_price) OVER w_grp AS prev_unit_price_2,
        LEAD(unit_price) OVER w_grp AS next_unit_price_2,
        ROW_NUMBER() OVER w_grp AS event_seq
    FROM with_windows
    WINDOW w_grp AS (ORDER BY unit_price, id)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER w_ordered AS event_seq_bucket,
        NTILE(10) OVER w_part AS prev_row_seq_bucket,
        NTILE(4) OVER (ORDER BY event_seq, id) AS event_seq_bucket_2
    FROM metrics
    WINDOW w_ordered AS (ORDER BY event_seq DESC, id), w_part AS (ORDER BY prev_row_seq, id)
),
metrics_order_items AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        next_quantity,
        next_row_seq,
        prev_unit_price_2,
        next_unit_price_2,
        event_seq_bucket,
        prev_row_seq_bucket,
        ROW_NUMBER() OVER (ORDER BY prev_row_seq DESC, id) AS seq_no,
        ROW_NUMBER() OVER (ORDER BY next_quantity, id) AS event_seq_2
    FROM scored
    WHERE event_seq_bucket >= 4
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM metrics_order_items
    INTERSECT ALL
    SELECT id
    FROM order_items
    WHERE order_id IN (4, 17)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics_order_items s
    INNER JOIN matched_keys k ON s.id = k.id
)
-- Grouped metrics
SELECT
    CASE WHEN next_unit_price_2 >= 330 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_unit_price_2_tier,
    PERCENTILE_APPROX(prev_row_seq_bucket, 0.5) AS prev_row_seq_bucket_p50,
    ROUND(AVG(next_quantity), 2) AS avg_next_quantity,
    COUNT(DISTINCT id) AS distinct_ids
FROM kept_rows
GROUP BY CASE WHEN next_unit_price_2 >= 330 THEN 'PREMIUM' ELSE 'STANDARD' END
