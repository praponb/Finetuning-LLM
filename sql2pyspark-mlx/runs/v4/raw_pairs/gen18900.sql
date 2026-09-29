WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(cost, 2) AS cost_rounded,
        TRIM(carrier) AS clean_carrier,
        COALESCE(carrier, 'NONE') AS resolved_carrier
    FROM shipments
    WHERE carrier = 'FedEx'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        cost_rounded,
        clean_carrier,
        resolved_carrier,
        SUM(cost_rounded) OVER (PARTITION BY carrier) AS carrier_sum_cost_rounded,
        NTILE(5) OVER w_ordered AS cost_bucket,
        NTILE(10) OVER w_part AS cost_bucket_2,
        ROW_NUMBER() OVER w_part_2 AS row_seq,
        NTILE(3) OVER w_part AS cost_bucket_3
    FROM base
    WINDOW w_ordered AS (PARTITION BY order_id ORDER BY cost DESC, id), w_part AS (PARTITION BY status ORDER BY cost, id), w_part_2 AS (PARTITION BY status ORDER BY cost DESC, id)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        cost_rounded,
        clean_carrier,
        resolved_carrier,
        carrier_sum_cost_rounded,
        cost_bucket,
        cost_bucket_2,
        row_seq,
        cost_bucket_3,
        LAST_VALUE(cost_rounded) IGNORE NULLS OVER (PARTITION BY status, carrier ORDER BY cost_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost_rounded,
        LEAD(cost_bucket_3, 2) OVER (ORDER BY shipped_date, id) AS next_cost_bucket_3
    FROM enriched
),
enriched_shipments AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(cost_rounded, 1, 0) OVER w_recent AS next_cost_rounded,
        LAST_VALUE(next_cost_bucket_3) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_cost_bucket_3,
        LEAD(cost_rounded, 1) OVER (PARTITION BY order_id ORDER BY cost_bucket, id) AS next_cost_rounded_2
    FROM metrics
    WINDOW w_recent AS (PARTITION BY order_id ORDER BY last_known_cost_rounded, id)
)
SELECT
    s.id,
    s.order_id,
    s.carrier,
    s.shipped_date,
    s.delivered_date,
    s.cost,
    s.clean_carrier,
    s.resolved_carrier,
    s.cost_bucket,
    (SELECT MIN(t2.quantity) FROM order_items t2 WHERE t2.order_id = s.order_id) AS min_order_item_quantity
FROM enriched_shipments s;
