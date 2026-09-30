WITH deduped AS (
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date DESC NULLS LAST, id) AS dedup_rank
        FROM shipments
    ) ranked
    WHERE dedup_rank = 1
),
with_windows AS (
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        ROUND(cost / SUM(cost) OVER (PARTITION BY order_id), 4) AS cost_share,
        LEAD(cost, 1) OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS next_cost,
        MIN(cost) OVER (PARTITION BY order_id) AS order_id_min_cost,
        LAST_VALUE(cost) IGNORE NULLS OVER w_hist AS last_known_cost,
        FIRST_VALUE(cost, TRUE) OVER w_hist AS first_known_cost
    FROM deduped
    WINDOW w_hist AS (PARTITION BY order_id ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_shipments AS (
    SELECT
        id,
        shipped_date,
        cost_share,
        order_id_min_cost,
        LAST_VALUE(next_cost, TRUE) OVER w_running AS last_known_next_cost,
        MIN(last_known_cost) OVER (PARTITION BY order_id) AS order_id_min_last_known_cost,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost_share, id) AS seq_no
    FROM with_windows
    WINDOW w_running AS (PARTITION BY order_id ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    SELECT *, GREATEST(last_known_next_cost, seq_no) AS greatest_last_known_next_cost_seq_no
    FROM with_windows_shipments
    WHERE shipped_date > TO_DATE('2023-09-12')
      AND order_id_min_last_known_cost IS NULL
),
scored AS (
    SELECT
        *,
        LAG(seq_no) OVER w_part AS prev_seq_no,
        LEAD(seq_no) OVER w_part AS next_seq_no,
        LAG(order_id_min_last_known_cost) OVER w_part AS prev_order_id_min_last_known_cost,
        ROW_NUMBER() OVER w_part AS seq_no_2
    FROM filtered
    WHERE cost_share > 0.28
    WINDOW w_part AS (ORDER BY greatest_last_known_next_cost_seq_no, id)
),
with_windows_shipments_2 AS (
    SELECT
        *,
        ROW_NUMBER() OVER w_part AS seq_no_3,
        ROW_NUMBER() OVER w_ordered AS row_seq,
        AVG(greatest_last_known_next_cost_seq_no) OVER (ORDER BY prev_seq_no, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS greatest_last_known_next_cost_seq_no_avg_5,
        SUM(prev_seq_no) OVER w_hist AS running_prev_seq_no,
        NTILE(10) OVER (ORDER BY next_seq_no, id) AS next_seq_no_bucket
    FROM scored
    WINDOW w_part AS (ORDER BY seq_no DESC, id), w_ordered AS (ORDER BY greatest_last_known_next_cost_seq_no DESC, id), w_hist AS (ORDER BY order_id_min_last_known_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered_shipments AS (
    SELECT
        *,
        CAST(last_known_next_cost AS INT) AS last_known_next_cost_int,
        CASE WHEN row_seq >= 7 THEN 'High' WHEN row_seq >= 3 THEN 'Medium' ELSE 'Low' END AS row_seq_level
    FROM with_windows_shipments_2
    WHERE shipped_date >= TO_DATE('2023-09-17')
)
SELECT
    id,
    shipped_date,
    cost_share,
    order_id_min_cost,
    last_known_next_cost,
    order_id_min_last_known_cost,
    seq_no,
    greatest_last_known_next_cost_seq_no,
    prev_seq_no
FROM filtered_shipments
ORDER BY shipped_date DESC NULLS FIRST, last_known_next_cost
