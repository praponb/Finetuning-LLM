WITH base AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        ROUND(cost, 0) AS cost_rounded,
        COALESCE(carrier, 'UNKNOWN') AS carrier_filled
    FROM shipments
    WHERE order_id IS NOT NULL
      AND status = 'LOST'
),
joined AS (
    SELECT
        a.*,
        d.status,
        d.shipping_fee,
        d.channel
    FROM base a
    JOIN orders d
        ON a.order_id <=> d.id
),
base_shipments AS (
    SELECT
        *,
        CASE WHEN channel = 'store' THEN 1 ELSE 0 END AS is_store,
        LENGTH(status) AS status_length
    FROM joined
    WHERE delivered_date > DATE '2023-04-26'
      AND MONTH(delivered_date) IN (4, 9, 12)
),
metrics AS (
    SELECT
        *,
        AVG(is_store) OVER (PARTITION BY status ORDER BY delivered_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_is_store,
        COUNT(is_store) OVER (PARTITION BY status ORDER BY delivered_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_is_store,
        MIN(is_store) OVER (PARTITION BY status ORDER BY delivered_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_is_store,
        ROW_NUMBER() OVER (PARTITION BY status, channel ORDER BY cost DESC, id ASC) AS row_seq
    FROM base_shipments
),
cleaned AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        cost_rounded,
        carrier_filled,
        status,
        channel,
        is_store,
        status_length,
        last7_avg_is_store,
        last7_count_is_store,
        row_seq,
        CEIL(last7_count_is_store) AS last7_count_is_store_whole
    FROM metrics
    WHERE delivered_date > DATE '2023-05-25'
      AND carrier IS NOT NULL
)
SELECT
    s.id,
    s.order_id,
    s.carrier,
    s.delivered_date,
    s.cost_rounded,
    s.status,
    s.channel,
    s.is_store,
    s.status_length,
    (SELECT AVG(x.unit_price) FROM order_items x WHERE x.order_id = s.order_id) AS avg_order_item_unit_price
FROM cleaned s
