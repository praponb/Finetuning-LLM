WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(amount, 0) AS amount_filled,
        DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_at_label,
        COALESCE(method, 'DIRECT') AS resolved_method
    FROM payments
    WHERE order_id IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(amount_filled) OVER (PARTITION BY method ORDER BY amount_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount_filled,
        RANK() OVER (PARTITION BY status, method ORDER BY amount_filled DESC NULLS LAST) AS amount_filled_rank,
        LAG(amount_filled, 2) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS prev_amount_filled
    FROM valid
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        paid_at,
        resolved_method,
        MAX(amount_filled_rank) OVER (PARTITION BY order_id, status ORDER BY amount_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_amount_filled_rank,
        MIN(amount_filled_rank) OVER (PARTITION BY order_id, status ORDER BY amount_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_amount_filled_rank,
        COUNT(amount_filled_rank) OVER (PARTITION BY order_id, status ORDER BY amount_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_amount_filled_rank,
        NTILE(10) OVER (PARTITION BY order_id ORDER BY running_amount_filled, id) AS running_amount_filled_bucket
    FROM enriched
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        paid_at,
        resolved_method,
        running_max_amount_filled_rank,
        running_min_amount_filled_rank,
        running_count_amount_filled_rank,
        ROUND(running_amount_filled_bucket / running_count_amount_filled_rank, 2) AS running_amount_filled_bucket_per_running_count_amount_filled_rank,
        COALESCE(running_count_amount_filled_rank, 0) AS running_count_amount_filled_rank_filled
    FROM scored
    WHERE paid_at >= DATE '2023-06-18'
      AND running_max_amount_filled_rank >= 7.1
)
-- Join a dimension and aggregate
SELECT
    o.status,
    o.channel,
    COUNT(*) AS num_rows,
    SUM(a.running_max_amount_filled_rank) AS total_running_max_amount_filled_rank
FROM filtered a
INNER JOIN orders o ON a.order_id = o.id
GROUP BY GROUPING SETS ((o.status, o.channel), (o.status), ())
ORDER BY num_rows DESC NULLS LAST;
