WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        FLOOR(quantity) AS quantity_whole,
        DAY(last_restock) AS last_restock_day
    FROM inventory
    WHERE last_restock >= DATE '2023-02-10'
      AND last_restock < '2025-09-08'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        last_restock_day,
        SUM(quantity) OVER (PARTITION BY store_id ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        FIRST_VALUE(last_restock_day) IGNORE NULLS OVER (ORDER BY quantity_whole ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last_restock_day,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY quantity ASC NULLS LAST) AS quantity_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity ASC, id ASC) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock_day DESC, id ASC) AS event_seq
    FROM valid
),
metrics_inventory AS (
    -- Ranking and gap features
    SELECT
        id,
        quantity_cume_dist,
        seq_no,
        event_seq,
        ROW_NUMBER() OVER w_part AS row_seq,
        FIRST_VALUE(seq_no) IGNORE NULLS OVER (ORDER BY first_known_last_restock_day ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_seq_no,
        NTILE(10) OVER w_part AS quantity_cume_dist_bucket
    FROM metrics
    WHERE seq_no = 4
    WINDOW w_part AS (ORDER BY quantity_cume_dist DESC, id ASC)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(event_seq) OVER (ORDER BY quantity_cume_dist_bucket ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS event_seq_avg_2_prior,
        SUM(event_seq) OVER (ORDER BY seq_no ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_event_seq,
        ROW_NUMBER() OVER (ORDER BY quantity_cume_dist DESC, id ASC) AS seq_no_2
    FROM metrics_inventory
),
metrics_inventory_2 AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(quantity_cume_dist) OVER w_ordered AS next_quantity_cume_dist,
        LEAD(quantity_cume_dist_bucket) OVER w_ordered AS next_quantity_cume_dist_bucket,
        ROW_NUMBER() OVER w_ordered AS seq_no_3,
        NTILE(3) OVER (ORDER BY seq_no_2 DESC, id ASC) AS seq_no_2_bucket
    FROM with_windows
    WINDOW w_ordered AS (ORDER BY quantity_cume_dist_bucket ASC, id ASC)
)
SELECT
    id,
    quantity_cume_dist,
    seq_no,
    event_seq,
    row_seq,
    first_known_seq_no,
    quantity_cume_dist_bucket,
    event_seq_avg_2_prior,
    cumulative_event_seq
FROM metrics_inventory_2
ORDER BY seq_no ASC NULLS FIRST
