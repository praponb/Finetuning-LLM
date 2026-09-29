WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN status IN ('DELIVERED', 'IN_TRANSIT') THEN 'Core' ELSE 'Other' END AS status_group,
        COALESCE(carrier, 'DIRECT') AS carrier_filled
    FROM shipments
    WHERE order_id IS NOT NULL
      AND MONTH(shipped_date) IN (7, 8, 9)
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.delivered_date,
        src.cost,
        src.status,
        src.carrier_filled,
        p.shipping_fee,
        p.discount
    FROM valid src
    LEFT JOIN orders p
        ON src.order_id = p.id
    WHERE p.store_id IS NOT NULL
),
enriched_shipments AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(cost) OVER (PARTITION BY order_id, status ORDER BY delivered_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_cost,
        AVG(cost) OVER (PARTITION BY order_id, status ORDER BY delivered_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_cost,
        MAX(cost) OVER (PARTITION BY order_id, status ORDER BY delivered_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_cost,
        MIN(cost) OVER (PARTITION BY order_id, status ORDER BY delivered_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_cost,
        NTILE(10) OVER (PARTITION BY order_id ORDER BY shipping_fee DESC, id) AS shipping_fee_bucket
    FROM enriched
),
joined AS (
    -- Attach orders attributes
    SELECT e.*, b.order_date
    FROM enriched_shipments e
    INNER JOIN orders b
        ON e.order_id <=> b.id
),
enriched_shipments_2 AS (
    -- Attach orders attributes
    SELECT
        e.*,
        b.shipping_fee AS order_shipping_fee,
        b.channel
    FROM joined e
    LEFT JOIN orders b
        ON e.order_id = b.id
)
SELECT
    s.id,
    s.order_id,
    s.delivered_date,
    s.status,
    s.shipping_fee,
    s.discount,
    s.last7_sum_cost,
    s.last7_avg_cost,
    s.last7_max_cost,
    CASE WHEN status RLIKE '^(LOST|DELIVERED)$' THEN 'GROUP_A' WHEN status RLIKE '^(IN_TRANSIT|RETURNED)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS status_zone,
    (SELECT MIN(o2.unit_price) FROM order_items o2 WHERE o2.order_id = s.order_id) AS min_order_item_unit_price
FROM enriched_shipments_2 s
ORDER BY discount DESC;
