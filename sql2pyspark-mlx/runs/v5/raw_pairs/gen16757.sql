WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(cost, 0) AS cost_filled
    FROM shipments
    WHERE shipped_date >= TO_DATE('2023-03-20')
),
joined AS (
    -- Attach orders attributes
    SELECT e.*, o.amount
    FROM cleaned e
    INNER JOIN orders o
        ON e.order_id = o.id
    WHERE o.store_id = 4
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY delivered_date DESC, id) AS latest_rank
    FROM joined
    QUALIFY latest_rank = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        shipped_date,
        cost,
        status,
        cost_filled,
        amount,
        latest_rank,
        AVG(cost_filled) OVER w_recent AS cost_filled_avg_5_prior,
        SUM(amount) OVER (PARTITION BY carrier ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        COUNT(cost_filled) OVER (PARTITION BY status, order_id) AS status_count_cost_filled
    FROM latest
    WINDOW w_recent AS (PARTITION BY status ORDER BY latest_rank, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
)
-- Join a dimension and aggregate
SELECT
    dim.status AS order_status,
    dim.channel,
    a.status AS shipment_status,
    COUNT(*) AS rows_total,
    SUM(a.cost_filled_avg_5_prior) AS total_cost_filled_avg_5_prior
FROM scored a
INNER JOIN orders dim ON a.order_id = dim.id
GROUP BY dim.status, dim.channel, a.status
ORDER BY rows_total DESC
