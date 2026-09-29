WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), delivered_date) days_since_delivered_date,
        COALESCE(carrier, 'NONE') carrier_filled
    FROM shipments
    WHERE order_id IS NOT NULL
      AND order_id = 35
),
with_dims AS (
    -- Attach orders attributes
    SELECT src.*, d.shipping_fee
    FROM valid src
    INNER JOIN orders d
        ON src.order_id = d.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(shipping_fee) OVER (PARTITION BY order_id, status ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_shipping_fee,
        SUM(shipping_fee) OVER (PARTITION BY order_id, status ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_shipping_fee,
        COUNT(shipping_fee) OVER (PARTITION BY order_id, status ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_shipping_fee,
        MAX(shipping_fee) OVER (PARTITION BY order_id, status ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_shipping_fee
    FROM with_dims
),
matched_keys AS (
    -- Set intersection on keys
    SELECT order_id, id
    FROM scored
    INTERSECT
    SELECT order_id, id
    FROM shipments
    WHERE delivered_date BETWEEN '2024-06-22' AND '2025-03-22'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.order_id = k.order_id AND s.id = k.id
),
enriched AS (
    -- Attach orders attributes
    SELECT
        a.*,
        p.amount,
        p.shipping_fee AS order_shipping_fee,
        p.order_date
    FROM kept_rows a
    LEFT JOIN orders p
        ON a.order_id = p.id
)
SELECT
    s.order_id,
    s.shipped_date,
    s.cost,
    s.status,
    s.carrier_filled,
    s.shipping_fee,
    s.running_sum_shipping_fee,
    s.running_max_shipping_fee,
    (SELECT SUM(p2.amount) FROM payments p2 WHERE p2.order_id = s.order_id) AS sum_payment_amount
FROM enriched s
ORDER BY sum_payment_amount DESC NULLS FIRST, order_id
