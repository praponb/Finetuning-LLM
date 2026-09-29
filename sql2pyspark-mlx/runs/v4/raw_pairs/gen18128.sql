WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        COALESCE(carrier, 'N/A') AS carrier_filled
    FROM shipments
    WHERE shipped_date >= DATE '2023-06-30'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(cost) OVER (PARTITION BY order_id) AS order_id_stddev_samp_cost,
        LEAD(cost, 1) OVER (PARTITION BY order_id ORDER BY shipped_date ASC, id ASC) AS next_cost,
        LEAD(cost, 1) OVER (PARTITION BY order_id ORDER BY cost ASC, id ASC) AS next_cost_2
    FROM filtered
),
enriched_shipments AS (
    -- Attach orders attributes
    SELECT
        a.*,
        p.discount,
        p.channel,
        p.shipping_fee
    FROM enriched a
    INNER JOIN orders p
        ON a.order_id = p.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER (ORDER BY next_cost_2 DESC, id ASC) AS next_cost_2_bucket,
        SUM(discount) OVER (ORDER BY order_id_stddev_samp_cost ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount,
        ROW_NUMBER() OVER (ORDER BY cost DESC, id ASC) AS event_seq
    FROM enriched_shipments
)
-- Running and moving aggregates
SELECT
    id,
    order_id,
    shipped_date,
    delivered_date,
    cost,
    carrier_filled,
    order_id_stddev_samp_cost,
    next_cost,
    next_cost_2,
    discount,
    channel,
    shipping_fee,
    next_cost_2_bucket,
    cumulative_discount,
    event_seq,
    LEAD(discount, 1) OVER (PARTITION BY channel ORDER BY shipping_fee ASC, id ASC) AS next_discount,
    FIRST_VALUE(shipping_fee) IGNORE NULLS OVER w_recent AS first_known_shipping_fee,
    LEAD(order_id_stddev_samp_cost, 2) OVER w_grp AS next_order_id_stddev_samp_cost,
    FIRST_VALUE(cumulative_discount, TRUE) OVER (PARTITION BY channel, order_id ORDER BY next_cost_2_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cumulative_discount,
    NTILE(4) OVER (ORDER BY cost DESC, id ASC) AS cost_bucket
FROM metrics
WINDOW w_recent AS (PARTITION BY channel ORDER BY next_cost_2 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY channel ORDER BY next_cost_2 ASC, id ASC)
