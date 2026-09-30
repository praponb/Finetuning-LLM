WITH joined AS (
    -- Attach orders attributes
    SELECT
        s.*,
        b.shipping_fee,
        b.discount
    FROM shipments s
    LEFT JOIN orders b
        ON s.order_id = b.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        shipping_fee,
        discount,
        SUM(discount) OVER (PARTITION BY carrier, status ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount,
        FIRST_VALUE(shipping_fee, TRUE) OVER (PARTITION BY carrier ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_shipping_fee
    FROM joined
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        shipping_fee,
        discount,
        cumulative_discount,
        first_known_shipping_fee,
        NTILE(3) OVER (PARTITION BY status ORDER BY shipping_fee, id) AS shipping_fee_bucket,
        ROW_NUMBER() OVER (ORDER BY first_known_shipping_fee, id) AS event_seq
    FROM with_windows
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY discount DESC NULLS FIRST, id) = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier, status ORDER BY first_known_shipping_fee DESC, id) AS rank_in_group
        FROM deduped
    ) ranked
    WHERE rank_in_group = 1
),
with_windows_shipments AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        shipping_fee,
        discount,
        cumulative_discount,
        first_known_shipping_fee,
        shipping_fee_bucket,
        event_seq,
        rank_in_group,
        COUNT(event_seq) OVER (PARTITION BY status) AS status_count_event_seq,
        AVG(discount) OVER (PARTITION BY order_id, carrier) AS order_id_avg_discount,
        NTILE(10) OVER (PARTITION BY status ORDER BY shipping_fee_bucket, id) AS shipping_fee_bucket_bucket
    FROM ranked
),
ranked_shipments AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY discount DESC, id) AS rank_in_group_2
        FROM with_windows_shipments
    ) ranked
    WHERE rank_in_group_2 = 1
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', shipped_date) AS year_start,
    COUNT(CASE WHEN status = 'IN_TRANSIT' THEN 1 END) AS in_transit_count,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT_IF(carrier = 'DHL') AS dhl_events,
    ROUND(SUM(event_seq) / NULLIF(COUNT(CASE WHEN carrier = 'Kerry' THEN 1 END), 0), 2) AS event_seq_per_kerry
FROM ranked_shipments
GROUP BY year_start
HAVING event_seq_per_kerry > 0
ORDER BY event_seq_per_kerry DESC
