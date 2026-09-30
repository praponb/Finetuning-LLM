WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY carrier, status ORDER BY cost DESC, id) AS cost_bucket,
        NTILE(10) OVER (PARTITION BY order_id ORDER BY cost, id) AS cost_bucket_2
    FROM shipments
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY status ORDER BY cost_bucket_2 DESC) AS cost_bucket_2_cume_dist,
        LAG(cost_bucket, 2) OVER w_hist AS prev_cost_bucket
    FROM with_windows
    WINDOW w_hist AS (PARTITION BY carrier ORDER BY cost_bucket_2, id)
),
joined AS (
    -- Attach orders attributes
    SELECT e.*, p.amount
    FROM scored e
    LEFT JOIN orders p
        ON e.order_id = p.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date DESC, id) AS row_seq,
        ROUND(cost_bucket_2 / SUM(cost_bucket_2) OVER (PARTITION BY order_id), 4) AS cost_bucket_2_share
    FROM joined
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY prev_cost_bucket DESC NULLS FIRST, id) AS rn
    FROM enriched
    QUALIFY rn = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        status,
        amount,
        cost_bucket_2_cume_dist,
        cost,
        cost_bucket_2_share
    FROM latest
) src
UNPIVOT (
    metric_value FOR kpi IN (amount AS AMOUNT, cost_bucket_2_cume_dist, cost AS COST, cost_bucket_2_share AS COST_BUCKET_2_SHARE_VAL)
)
