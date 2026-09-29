WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id) AS row_num
        FROM payments
    ) ranked
    WHERE row_num = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY method ORDER BY amount DESC NULLS FIRST) amount_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY status, method ORDER BY row_num, id) event_seq,
        ROUND(amount / SUM(amount) OVER (PARTITION BY status), 4) amount_share
    FROM deduped
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY row_num DESC, id) row_seq,
        FIRST_VALUE(method) IGNORE NULLS OVER (PARTITION BY method, order_id ORDER BY amount_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_method,
        SUM(amount_cume_dist) OVER w_running cumulative_amount_cume_dist
    FROM enriched
    WINDOW w_running AS (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(row_seq) OVER (PARTITION BY order_id ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) row_seq_sum_30d,
        LAST_VALUE(amount_share) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_amount_share,
        LEAD(amount_share, 1, 0) OVER (PARTITION BY method, status ORDER BY amount, id) next_amount_share,
        CUME_DIST() OVER (PARTITION BY order_id, method ORDER BY event_seq NULLS LAST) event_seq_cume_dist,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY status), 4) event_seq_share
    FROM scored
    WHERE event_seq < 5
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT method, order_id
    FROM metrics
    INTERSECT ALL
    SELECT method, order_id
    FROM payments
    WHERE status != 'FAILED'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    JOIN remaining_keys k ON s.method = k.method AND s.order_id = k.order_id
)
-- Multi-level aggregation
SELECT
    method,
    order_id,
    SUM(CASE WHEN status = 'REFUNDED' THEN amount ELSE 0 END) AS refunded_amount,
    APPROX_PERCENTILE(event_seq_share, ARRAY(0.25, 0.5, 0.75)) AS event_seq_share_quartiles,
    MAX(event_seq) AS max_event_seq,
    SORT_ARRAY(COLLECT_SET(method)) AS method_list,
    GROUPING_ID() AS grouping_id
FROM kept_rows
GROUP BY GROUPING SETS ((method, order_id), (method), ())
HAVING refunded_amount > 0
