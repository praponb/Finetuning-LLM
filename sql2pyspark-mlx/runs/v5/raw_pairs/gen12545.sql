WITH joined AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.status,
        e.channel,
        e.amount,
        e.discount,
        e.shipping_fee,
        p.age,
        p.city,
        p.email
    FROM orders e
    LEFT JOIN customers p
        ON e.customer_id = p.id
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM joined s
    WHERE EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.order_id = s.id
              AND x.product_id IS NOT NULL
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.cost > 33
        )
),
latest_match AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.status,
        s.channel,
        s.discount,
        s.age,
        s.city,
        s.email,
        x.shipment_shipped_date,
        x.shipment_carrier
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.shipped_date AS shipment_shipped_date, c.carrier AS shipment_carrier
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.delivered_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('WEEK', shipment_shipped_date) AS week_start,
        COUNT(DISTINCT channel) AS distinct_channels,
        STDDEV_SAMP(discount) AS discount_stddev,
        SUM(CASE WHEN status = 'SHIPPED' THEN age ELSE 0 END) AS shipped_age,
        SUM(CASE WHEN channel = 'mobile' THEN age ELSE 0 END) AS mobile_age
    FROM latest_match
    GROUP BY DATE_TRUNC('WEEK', shipment_shipped_date)
)
SELECT
    week_start,
    distinct_channels,
    discount_stddev,
    shipped_age,
    mobile_age
FROM stats
ORDER BY week_start ASC
LIMIT 10
