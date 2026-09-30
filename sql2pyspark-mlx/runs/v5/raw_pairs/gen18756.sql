WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(quantity, 0) quantity_rounded
    FROM order_items
    WHERE unit_price < 300
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT id, unit_price
    FROM (
        SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY quantity_rounded DESC NULLS LAST) AS latest_rank
        FROM cleaned
    ) x
    WHERE latest_rank = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        unit_price,
        LAST_VALUE(unit_price, TRUE) OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_unit_price,
        SUM(unit_price) OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_unit_price,
        SUM(unit_price) OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_unit_price,
        DENSE_RANK() OVER (ORDER BY unit_price DESC NULLS FIRST) unit_price_dense_rank
    FROM deduped
    WHERE unit_price >= 270
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        unit_price,
        last_known_unit_price,
        running_unit_price,
        unit_price_dense_rank,
        ROUND(unit_price / unit_price_dense_rank, 2) unit_price_per_unit_price_dense_rank
    FROM scored
    WHERE unit_price_dense_rank = 8
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY unit_price_dense_rank, id) row_seq,
        ROW_NUMBER() OVER (ORDER BY last_known_unit_price, id) seq_no,
        NTILE(5) OVER (ORDER BY last_known_unit_price DESC, id) last_known_unit_price_bucket,
        NTILE(5) OVER (ORDER BY running_unit_price DESC, id) running_unit_price_bucket
    FROM base
),
metrics_order_items AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(running_unit_price) OVER w_ordered running_count_running_unit_price,
        AVG(running_unit_price) OVER w_ordered running_avg_running_unit_price,
        MAX(running_unit_price) OVER w_ordered running_max_running_unit_price,
        SUM(unit_price) OVER w_ordered cumulative_unit_price
    FROM metrics
    WINDOW w_ordered AS (ORDER BY running_unit_price_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        unit_price,
        last_known_unit_price,
        unit_price_dense_rank,
        unit_price_per_unit_price_dense_rank,
        row_seq,
        seq_no,
        last_known_unit_price_bucket,
        running_unit_price_bucket,
        running_count_running_unit_price,
        cumulative_unit_price,
        running_unit_price * seq_no running_unit_price_times_seq_no,
        CAST(unit_price AS INT) unit_price_int
    FROM metrics_order_items
    WHERE running_avg_running_unit_price >= 500
)
SELECT
    id,
    unit_price,
    last_known_unit_price,
    unit_price_dense_rank,
    row_seq,
    seq_no,
    last_known_unit_price_bucket,
    running_count_running_unit_price,
    cumulative_unit_price
FROM valid;
