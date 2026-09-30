WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(units) OVER (PARTITION BY product_id ORDER BY revenue ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS units_avg_3,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units DESC, id ASC) AS event_seq
    FROM store_sales
    WHERE store_id IN (1, 4, 5)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        units,
        revenue,
        units_avg_3,
        LAG(event_seq) OVER w_ordered AS prev_event_seq,
        LAG(units) OVER w_ordered AS prev_units,
        ROW_NUMBER() OVER w_ordered AS seq_no
    FROM with_windows
    WINDOW w_ordered AS (PARTITION BY product_id ORDER BY event_seq ASC, id ASC)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        prev_event_seq - seq_no AS prev_event_seq_minus_seq_no,
        ROUND(prev_event_seq / units, 2) AS prev_event_seq_per_units
    FROM metrics
    WHERE seq_no != 8
),
joined AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.product_id,
        e.units,
        e.prev_event_seq_minus_seq_no,
        e.prev_event_seq_per_units,
        p.stock_qty,
        p.product_name
    FROM cleaned e
    LEFT JOIN products p
        ON e.product_id = p.id
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY prev_event_seq_per_units DESC, id ASC) AS rn
        FROM joined
    ) x
    WHERE rn = 1
),
with_windows_store_sales AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER w_grp AS row_seq,
        SUM(units) OVER (ORDER BY rn ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_units,
        AVG(stock_qty) OVER (PARTITION BY product_id ORDER BY prev_event_seq_per_units ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS stock_qty_avg_5_prior
    FROM deduped
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY prev_event_seq_per_units ASC, id ASC)
),
with_windows_store_sales_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        prev_event_seq_minus_seq_no,
        prev_event_seq_per_units,
        stock_qty,
        product_name,
        rn,
        row_seq,
        running_units,
        SUM(row_seq) OVER (PARTITION BY product_id ORDER BY running_units ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_row_seq,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY prev_event_seq_per_units DESC) AS prev_event_seq_per_units_cume_dist,
        RANK() OVER (PARTITION BY product_id ORDER BY stock_qty_avg_5_prior DESC NULLS LAST) AS stock_qty_avg_5_prior_rank
    FROM with_windows_store_sales
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN prev_event_seq_per_units >= 79 THEN 'HIGH' ELSE 'LOW' END AS prev_event_seq_per_units_tier,
    ROUND(AVG(stock_qty_avg_5_prior_rank), 2) AS avg_stock_qty_avg_5_prior_rank,
    MIN(row_seq) AS min_row_seq,
    PERCENTILE_APPROX(prev_event_seq_minus_seq_no, ARRAY(0.25, 0.5, 0.75)) AS prev_event_seq_minus_seq_no_quartiles,
    PERCENTILE_APPROX(row_seq, ARRAY(0.25, 0.5, 0.75)) AS row_seq_quartiles,
    COUNT(*) AS num_rows,
    COUNT(*) AS cnt
FROM with_windows_store_sales_2
GROUP BY prev_event_seq_per_units_tier
ORDER BY num_rows ASC NULLS LAST;
