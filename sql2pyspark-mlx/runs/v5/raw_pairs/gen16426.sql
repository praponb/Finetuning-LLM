WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        status,
        channel,
        amount + shipping_fee AS amount_with_shipping,
        COALESCE(shipping_fee, 0) AS shipping_fee_filled
    FROM orders
    WHERE customer_id IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        status,
        channel,
        amount_with_shipping,
        shipping_fee_filled
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, status ORDER BY amount_with_shipping DESC NULLS FIRST, id) = 1
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.carrier IN ('FedEx', 'Kerry', 'Flash')
        )
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN channel = 'web' THEN 1 ELSE 0 END AS is_web,
        COALESCE(channel, 'NONE') AS resolved_channel
    FROM kept
    WHERE shipping_fee_filled < 21
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN amount_with_shipping >= 78 THEN 'HIGH' ELSE 'LOW' END AS amount_with_shipping_tier,
        status,
        SORT_ARRAY(COLLECT_SET(status)) AS status_list,
        ROUND(SUM(shipping_fee_filled) / NULLIF(COUNT(CASE WHEN channel = 'mobile' THEN 1 END), 0), 2) AS shipping_fee_filled_per_mobile,
        COUNT(*) AS event_count,
        GROUPING(CASE WHEN amount_with_shipping >= 78 THEN 'HIGH' ELSE 'LOW' END) AS gid
    FROM cleaned
    GROUP BY CUBE (CASE WHEN amount_with_shipping >= 78 THEN 'HIGH' ELSE 'LOW' END, status)
)
SELECT
    amount_with_shipping_tier,
    status,
    status_list,
    shipping_fee_filled_per_mobile,
    event_count,
    gid,
    CASE WHEN status RLIKE '^(PENDING|CANCELLED)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS status_zone
FROM totals
