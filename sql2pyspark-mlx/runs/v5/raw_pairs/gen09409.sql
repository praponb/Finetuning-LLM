WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(cost) OVER (PARTITION BY order_id) AS order_id_max_cost,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost DESC, id ASC) AS seq_no,
        PERCENT_RANK() OVER (PARTITION BY carrier ORDER BY cost ASC NULLS FIRST) AS cost_percent_rank,
        AVG(cost) OVER w_ordered AS cost_avg_3,
        LEAD(cost, 1) OVER w_ordered_2 AS next_cost
    FROM shipments
    WINDOW w_ordered AS (PARTITION BY carrier ORDER BY delivered_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_ordered_2 AS (ORDER BY delivered_date ASC, id ASC)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(seq_no) OVER w_recent AS running_avg_seq_no,
        SUM(seq_no) OVER w_recent AS running_sum_seq_no,
        MAX(seq_no) OVER w_recent AS running_max_seq_no,
        MIN(seq_no) OVER w_recent AS running_min_seq_no
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY carrier ORDER BY cost_percent_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(seq_no) OVER (PARTITION BY status ORDER BY delivered_date ASC, id ASC) AS prev_seq_no,
        LEAD(seq_no) OVER (PARTITION BY status ORDER BY delivered_date ASC, id ASC) AS next_seq_no,
        LEAD(cost_avg_3) OVER (PARTITION BY status ORDER BY delivered_date ASC, id ASC) AS next_cost_avg_3,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date ASC, id ASC) AS seq_no_2,
        NTILE(5) OVER (PARTITION BY status ORDER BY running_min_seq_no DESC, id ASC) AS running_min_seq_no_bucket
    FROM scored
),
remaining_keys AS (
    -- Set difference on keys
    SELECT order_id, status
    FROM metrics
    EXCEPT ALL
    SELECT order_id, status
    FROM shipments
    WHERE order_id IS NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys k ON s.order_id = k.order_id AND s.status = k.status
),
joined AS (
    -- Attach orders attributes
    SELECT e.*, b.discount
    FROM reconciled e
    INNER JOIN orders b
        ON e.order_id <=> b.id
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.carrier,
        src.shipped_date,
        src.delivered_date,
        src.cost,
        src.order_id_max_cost,
        src.cost_avg_3,
        src.next_cost,
        src.running_avg_seq_no,
        src.running_sum_seq_no,
        src.next_seq_no,
        src.running_min_seq_no_bucket,
        src.discount,
        o.amount,
        o.status AS order_status
    FROM joined src
    LEFT JOIN orders o
        ON src.order_id = o.id
    WHERE o.shipping_fee BETWEEN 15 AND 26
)
-- Join a dimension and aggregate
SELECT
    d.status,
    src.order_status,
    COUNT(*) AS rows_total,
    MAX(src.order_id_max_cost) AS max_order_id_max_cost,
    MAX(src.cost_avg_3) AS max_cost_avg_3,
    GROUPING_ID() AS grouping_level
FROM enriched src
INNER JOIN orders d ON src.order_id = d.id
GROUP BY GROUPING SETS ((d.status, src.order_status), (d.status), ());
