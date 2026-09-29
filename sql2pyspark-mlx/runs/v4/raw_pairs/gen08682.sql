WITH with_dims AS (
    -- Attach orders attributes
    SELECT
        src.*,
        d.discount,
        d.channel
    FROM shipments src
    INNER JOIN orders d
        ON src.order_id = d.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(discount) OVER (PARTITION BY channel, status ORDER BY delivered_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS discount_avg_5_prior,
        NTILE(10) OVER (PARTITION BY status ORDER BY cost DESC, id) AS cost_bucket,
        SUM(discount) OVER (PARTITION BY status, order_id ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount,
        NTILE(4) OVER (PARTITION BY status ORDER BY discount, id) AS discount_bucket
    FROM with_dims
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.*,
        b.order_date,
        b.status AS order_status
    FROM metrics src
    INNER JOIN orders b
        ON src.order_id = b.id
),
enriched_shipments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        discount,
        channel,
        discount_avg_5_prior,
        cost_bucket,
        running_discount,
        discount_bucket,
        order_date,
        order_status,
        FIRST_VALUE(discount_avg_5_prior) IGNORE NULLS OVER w_recent AS first_known_discount_avg_5_prior,
        LEAD(discount, 2) OVER (PARTITION BY order_status ORDER BY cost_bucket, id) AS next_discount,
        FIRST_VALUE(cost_bucket, TRUE) OVER (ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost_bucket
    FROM enriched
    WINDOW w_recent AS (ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    carrier,
    shipped_date,
    delivered_date,
    cost,
    discount,
    discount_avg_5_prior,
    cost_bucket,
    running_discount
FROM enriched_shipments
ORDER BY cost_bucket DESC NULLS LAST;
