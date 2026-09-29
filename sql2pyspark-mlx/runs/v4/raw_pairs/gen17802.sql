WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM shipments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY cost, id) = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY status, order_id ORDER BY cost, id) cost_bucket,
        LAST_VALUE(cost, TRUE) OVER (PARTITION BY carrier, status ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_cost
    FROM latest
),
enriched_shipments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY order_id ORDER BY last_known_cost, id) last_known_cost_bucket,
        ROW_NUMBER() OVER (ORDER BY shipped_date, id) row_seq,
        SUM(cost) OVER (PARTITION BY order_id ORDER BY cost_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_cost,
        LEAD(cost_bucket, 1) OVER (ORDER BY cost_bucket, id) next_cost_bucket,
        NTILE(3) OVER (PARTITION BY status ORDER BY cost_bucket DESC, id) cost_bucket_bucket
    FROM enriched
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        cost_bucket,
        last_known_cost,
        last_known_cost_bucket,
        cumulative_cost,
        next_cost_bucket,
        cost * cost_bucket cost_times_cost_bucket,
        COALESCE(carrier, 'NONE') carrier_clean
    FROM enriched_shipments
    WHERE cost > 69
)
SELECT
    id,
    order_id,
    carrier,
    shipped_date,
    delivered_date,
    cost_bucket,
    last_known_cost,
    last_known_cost_bucket,
    cumulative_cost
FROM valid
ORDER BY id DESC;
