WITH valid AS (
    SELECT *, quantity + unit_price AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND quantity <= 4
),
enriched AS (
    SELECT
        src.*,
        d.product_name,
        d.cost
    FROM valid src
    LEFT JOIN products d
        ON src.product_id <=> d.id
    WHERE d.supplier_id = 1
),
scored AS (
    SELECT
        id,
        product_id,
        unit_price,
        line_total,
        CORR(quantity, unit_price) OVER (PARTITION BY order_id) AS corr_quantity_unit_price,
        PERCENT_RANK() OVER (ORDER BY line_total NULLS FIRST) AS line_total_percent_rank
    FROM enriched
),
scored_order_items AS (
    SELECT
        *,
        LEAD(line_total_percent_rank) OVER w_recent AS next_line_total_percent_rank,
        LAG(line_total_percent_rank) OVER w_recent AS prev_line_total_percent_rank,
        LAG(line_total) OVER w_recent AS prev_line_total,
        ROW_NUMBER() OVER w_recent AS row_seq,
        NTILE(5) OVER (ORDER BY line_total DESC, id) AS line_total_bucket
    FROM scored
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY unit_price, id)
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY line_total DESC, id) AS latest_rank
        FROM scored_order_items
    ) x
    WHERE latest_rank = 1
),
metrics AS (
    SELECT
        *,
        SUM(corr_quantity_unit_price) OVER (PARTITION BY product_id ORDER BY line_total, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_corr_quantity_unit_price,
        MIN(line_total_percent_rank) OVER w_hist AS product_id_min_line_total_percent_rank,
        SUM(prev_line_total) OVER w_hist AS product_id_sum_prev_line_total
    FROM deduped
    WINDOW w_hist AS (PARTITION BY product_id)
)
SELECT
    id,
    product_id,
    unit_price,
    line_total_percent_rank,
    next_line_total_percent_rank,
    prev_line_total_percent_rank,
    prev_line_total,
    row_seq,
    product_id_min_line_total_percent_rank,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id_min_line_total_percent_rank, id) AS row_seq_2,
    STDDEV_SAMP(unit_price) OVER w_recent AS product_id_stddev_samp_unit_price,
    NTILE(10) OVER (ORDER BY unit_price, id) AS unit_price_bucket
FROM metrics
WINDOW w_recent AS (PARTITION BY product_id);
