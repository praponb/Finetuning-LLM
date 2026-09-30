WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_TRUNC('YEAR', delivered_date) AS delivered_date_period,
        ROUND(cost, 1) AS cost_rounded
    FROM shipments
    WHERE shipped_date >= DATE '2023-03-16'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        status,
        SUM(cost) OVER (ORDER BY cost_rounded ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY cost ASC NULLS FIRST) AS cost_dense_rank,
        NTILE(10) OVER (PARTITION BY carrier ORDER BY cost DESC, id ASC) AS cost_bucket
    FROM valid
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, DATE_TRUNC('year', shipped_date) AS shipped_date_period
    FROM metrics
    WHERE delivered_date >= DATE '2023-08-23'
      AND order_id IS NOT NULL
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.delivered_date,
        a.cost,
        a.cumulative_cost,
        a.cost_dense_rank,
        o.channel,
        o.amount
    FROM cleaned a
    LEFT JOIN orders o
        ON a.order_id = o.id
    WHERE o.amount >= 1430
),
with_dims_shipments AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.cumulative_cost,
        s.cost_dense_rank,
        s.channel,
        s.amount,
        o.discount,
        o.amount AS order_amount
    FROM with_dims s
    INNER JOIN orders o
        ON s.order_id = o.id
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.cumulative_cost,
        src.channel,
        src.discount,
        src.order_amount,
        d.amount AS order_amount_2
    FROM with_dims_shipments src
    LEFT JOIN orders d
        ON src.order_id = d.id
)
SELECT
    s.id,
    s.order_id,
    s.channel,
    s.discount,
    s.order_amount,
    s.order_amount_2,
    CASE WHEN channel RLIKE '^(partner|store)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS channel_zone,
    (SELECT MIN(x.amount) FROM payments x WHERE x.order_id = s.order_id) AS min_payment_amount
FROM enriched s
ORDER BY min_payment_amount DESC;
