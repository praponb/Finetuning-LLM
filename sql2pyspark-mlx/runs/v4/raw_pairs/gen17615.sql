WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        DATE_SUB(sale_date, 52) AS sale_date_minus_52d
    FROM store_sales
    WHERE store_id = 4
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        sale_date_minus_52d,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date_minus_52d, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date_minus_52d DESC, id) AS event_seq,
        ROW_NUMBER() OVER w_part AS seq_no,
        ROW_NUMBER() OVER w_recent AS row_seq_2
    FROM base
    WINDOW w_part AS (ORDER BY sale_date DESC, id), w_recent AS (ORDER BY sale_date, id)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(event_seq, 1) OVER (PARTITION BY store_id ORDER BY event_seq, id) AS next_event_seq,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY event_seq DESC, id) AS event_seq_bucket,
        NTILE(5) OVER (PARTITION BY product_id ORDER BY event_seq DESC, id) AS event_seq_bucket_2
    FROM metrics
)
-- Multi-level aggregation
SELECT
    store_id,
    APPROX_PERCENTILE(event_seq_bucket_2, 0.5) AS event_seq_bucket_2_p50,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_windows
GROUP BY store_id
HAVING distinct_ids < 1
ORDER BY store_id DESC
