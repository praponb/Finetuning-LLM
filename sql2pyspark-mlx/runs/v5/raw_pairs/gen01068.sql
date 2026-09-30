WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(quantity / unit_price, 2) AS line_total,
        quantity + unit_price AS line_total_2
    FROM order_items
    WHERE product_id IS NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        quantity,
        line_total,
        line_total_2,
        AVG(quantity) OVER w_part AS quantity_avg_6,
        ROW_NUMBER() OVER w_grp AS seq_no,
        LAST_VALUE(unit_price, TRUE) OVER w_ordered AS last_known_unit_price
    FROM filtered
    WINDOW w_part AS (PARTITION BY product_id ORDER BY unit_price ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY product_id ORDER BY unit_price DESC, id ASC), w_ordered AS (PARTITION BY product_id ORDER BY line_total ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(5) OVER (ORDER BY last_known_unit_price DESC, id ASC) AS last_known_unit_price_bucket,
        ROW_NUMBER() OVER (ORDER BY seq_no DESC, id ASC) AS seq_no_2,
        FIRST_VALUE(quantity_avg_6, TRUE) OVER (ORDER BY quantity_avg_6 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity_avg_6,
        NTILE(4) OVER (ORDER BY seq_no DESC, id ASC) AS seq_no_bucket
    FROM with_windows
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        quantity,
        line_total,
        seq_no,
        last_known_unit_price_bucket,
        seq_no_bucket,
        AVG(last_known_unit_price) OVER (ORDER BY last_known_unit_price_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_last_known_unit_price,
        MIN(last_known_unit_price) OVER (ORDER BY last_known_unit_price_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_last_known_unit_price,
        COUNT(last_known_unit_price) OVER (ORDER BY last_known_unit_price_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_last_known_unit_price
    FROM enriched
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        line_total,
        seq_no,
        seq_no_bucket,
        running_avg_last_known_unit_price,
        running_count_last_known_unit_price,
        COALESCE(seq_no_bucket, 0) AS seq_no_bucket_filled
    FROM scored
    WHERE seq_no != 8
),
with_windows_order_items AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(running_avg_last_known_unit_price) OVER w_part AS prev_running_avg_last_known_unit_price,
        LAG(seq_no_bucket_filled) OVER w_part AS prev_seq_no_bucket_filled,
        LEAD(seq_no_bucket_filled) OVER w_part AS next_seq_no_bucket_filled,
        ROW_NUMBER() OVER w_part AS seq_no_2,
        ROW_NUMBER() OVER (ORDER BY running_avg_last_known_unit_price DESC, id ASC) AS row_seq
    FROM base
    WINDOW w_part AS (ORDER BY running_avg_last_known_unit_price ASC, id ASC)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, prev_running_avg_last_known_unit_price / NULLIF(line_total, 0) AS prev_running_avg_last_known_unit_price_per_line_total
    FROM with_windows_order_items
    WHERE next_seq_no_bucket_filled > 66
)
SELECT
    id,
    line_total,
    seq_no,
    running_avg_last_known_unit_price,
    running_count_last_known_unit_price,
    seq_no_bucket_filled,
    prev_seq_no_bucket_filled,
    seq_no_2,
    prev_running_avg_last_known_unit_price_per_line_total
FROM cleaned
ORDER BY running_count_last_known_unit_price ASC
