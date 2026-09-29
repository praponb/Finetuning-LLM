WITH joined AS (
    -- Attach orders attributes
    SELECT s.*, o.status AS order_status
    FROM shipments s
    INNER JOIN orders o
        ON s.order_id = o.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_TRUNC('month', shipped_date) shipped_date_period
    FROM joined
    WHERE MONTH(delivered_date) IN (7, 8, 9)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        shipped_date_period,
        ROUND(cost / SUM(cost) OVER (PARTITION BY status, order_id), 4) cost_share,
        DENSE_RANK() OVER (PARTITION BY status, order_id ORDER BY cost DESC NULLS LAST) cost_dense_rank,
        SUM(cost) OVER (PARTITION BY carrier ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_cost
    FROM filtered
    WHERE YEAR(delivered_date) = 2024
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(cumulative_cost, 2, 0) OVER w_ordered prev_cumulative_cost,
        NTILE(3) OVER (PARTITION BY order_id ORDER BY cost_dense_rank DESC, id) cost_dense_rank_bucket,
        STDDEV_SAMP(cost) OVER w_hist order_id_stddev_samp_cost,
        RANK() OVER (PARTITION BY carrier ORDER BY cost DESC NULLS LAST) cost_rank
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY status ORDER BY cost_share, id), w_hist AS (PARTITION BY order_id)
),
enriched_shipments AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.shipped_date,
        s.delivered_date,
        s.status,
        s.cost_share,
        s.cost_dense_rank,
        s.cumulative_cost,
        s.cost_dense_rank_bucket,
        s.cost_rank,
        b.order_date
    FROM scored s
    INNER JOIN orders b
        ON s.order_id = b.id
),
joined_shipments AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.shipped_date,
        s.cost_share,
        s.cumulative_cost,
        s.cost_dense_rank_bucket,
        s.cost_rank,
        d.amount,
        d.discount
    FROM enriched_shipments s
    INNER JOIN orders d
        ON s.order_id = d.id
)
SELECT
    id,
    cumulative_cost,
    amount
FROM joined_shipments;
