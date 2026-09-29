WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, quantity * unit_price AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
),
base_order_items AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        line_total + quantity AS line_total_plus_quantity
    FROM base
    WHERE product_id IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        line_total_plus_quantity,
        ROW_NUMBER() OVER (ORDER BY line_total_plus_quantity ASC, id ASC) AS row_seq,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY line_total_plus_quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        NTILE(3) OVER (PARTITION BY product_id ORDER BY line_total_plus_quantity ASC, id ASC) AS line_total_plus_quantity_bucket
    FROM base_order_items
),
base_order_items_2 AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        quantity,
        row_seq,
        cumulative_quantity,
        LEAST(quantity, row_seq) AS least_quantity_row_seq,
        ROUND(unit_price / line_total_plus_quantity_bucket, 2) AS unit_price_per_line_total_plus_quantity_bucket
    FROM with_windows
    WHERE product_id IS NOT NULL
)
-- Ranking and gap features
SELECT
    *,
    AVG(row_seq) OVER (PARTITION BY product_id ORDER BY row_seq ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS row_seq_avg_6,
    SUM(cumulative_quantity) OVER (PARTITION BY product_id ORDER BY unit_price_per_line_total_plus_quantity_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cumulative_quantity,
    PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY least_quantity_row_seq ASC NULLS LAST) AS least_quantity_row_seq_percent_rank
FROM base_order_items_2
