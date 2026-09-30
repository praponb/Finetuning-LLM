WITH with_dims AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.store_id,
        a.order_date,
        a.status,
        a.shipping_fee,
        d.store_name,
        d.opened_date,
        d.sqft
    FROM orders a
    LEFT JOIN stores d
        ON a.store_id <=> d.id
    WHERE d.region = 'North'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        order_date,
        status,
        shipping_fee,
        store_name,
        sqft,
        MAX(sqft) OVER w_grp AS status_max_sqft,
        ROW_NUMBER() OVER w_recent AS event_seq,
        NTILE(10) OVER (ORDER BY shipping_fee DESC, id) AS shipping_fee_bucket,
        FIRST_VALUE(sqft) IGNORE NULLS OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft,
        AVG(sqft) OVER (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS sqft_avg_6_prior
    FROM with_dims
    WINDOW w_grp AS (PARTITION BY status), w_recent AS (ORDER BY order_date DESC, id)
),
latest_match AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.carrier,
        latest.shipped_date
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.carrier, c.shipped_date
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.shipped_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.store_id,
        s.store_name,
        s.status_max_sqft,
        s.event_seq,
        s.shipping_fee_bucket,
        s.first_known_sqft,
        s.sqft_avg_6_prior,
        s.carrier,
        s.shipped_date,
        b.region
    FROM latest_match s
    LEFT JOIN stores b
        ON s.store_id <=> b.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        region,
        carrier,
        status_max_sqft
    FROM enriched
) src
PIVOT (
    SUM(status_max_sqft) AS value_sum,
    COUNT(1) AS n
    FOR carrier IN ('DHL' AS dhl, 'UPS' AS ups)
)
