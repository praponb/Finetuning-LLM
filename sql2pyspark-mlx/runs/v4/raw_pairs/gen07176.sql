WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DAY(delivered_date) AS delivered_date_day,
        INITCAP(TRIM(carrier)) AS carrier_title
    FROM shipments
    WHERE order_id IS NOT NULL
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        e.*,
        b.status AS order_status,
        b.amount,
        b.shipping_fee
    FROM cleaned e
    LEFT JOIN orders b
        ON e.order_id = b.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(carrier, ', ', status) AS carrier_status,
        COALESCE(carrier, 'OTHER') AS resolved_carrier
    FROM with_dims
    WHERE order_status IS NOT NULL
      AND delivered_date BETWEEN '2023-06-13' AND '2023-12-05'
),
daily AS (
    -- Grouped metrics
    SELECT
        order_id,
        DATE_TRUNC('QUARTER', shipped_date) AS quarter_start,
        SIZE(COLLECT_SET(carrier)) AS num_carriers,
        SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list,
        STDDEV_SAMP(cost) AS cost_stddev
    FROM valid
    GROUP BY GROUPING SETS ((order_id, DATE_TRUNC('QUARTER', shipped_date)), (order_id), ())
)
SELECT
    s.order_id,
    s.quarter_start,
    s.num_carriers,
    s.carrier_list,
    s.cost_stddev,
    (SELECT SUM(t2.amount) FROM payments t2 WHERE t2.order_id = s.order_id) AS sum_payment_amount
FROM daily s
