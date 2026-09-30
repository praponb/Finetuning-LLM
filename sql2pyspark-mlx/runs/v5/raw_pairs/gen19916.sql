WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM shipments
    QUALIFY RANK() OVER (PARTITION BY order_id ORDER BY shipped_date DESC) = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(cost) OVER (PARTITION BY order_id ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost,
        AVG(cost) OVER (PARTITION BY status ORDER BY delivered_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS cost_avg_3_prior
    FROM deduped
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.shipped_date,
        src.delivered_date,
        src.cost,
        src.cost_avg_3_prior,
        b.discount,
        b.shipping_fee
    FROM enriched src
    LEFT JOIN orders b
        ON src.order_id = b.id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        shipped_date,
        delivered_date,
        cost,
        cost_avg_3_prior,
        discount,
        shipping_fee,
        LEAST(cost, cost_avg_3_prior) AS least_cost_cost_avg_3_prior
    FROM with_dims
    WHERE shipped_date > DATE '2023-02-19'
),
report AS (
    SELECT
        id,
        discount,
        shipping_fee,
        least_cost_cost_avg_3_prior
    FROM base
    ORDER BY id, shipping_fee DESC
)
SELECT
    discount,
    shipping_fee,
    least_cost_cost_avg_3_prior
FROM report;
