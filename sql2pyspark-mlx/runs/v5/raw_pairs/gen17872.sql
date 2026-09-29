WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        store_id,
        status,
        discount,
        shipping_fee,
        discount + amount AS discount_plus_amount,
        shipping_fee + discount AS shipping_fee_plus_discount
    FROM orders
    WHERE order_date >= DATE '2023-04-19'
      AND store_id IS NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.delivered_date >= '2025-02-05'
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(discount_plus_amount, 1) OVER (PARTITION BY store_id, customer_id ORDER BY discount, id) AS next_discount_plus_amount,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY discount DESC, id) AS event_seq
    FROM eligible
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.store_id,
        src.status,
        src.shipping_fee,
        src.discount_plus_amount,
        src.next_discount_plus_amount,
        p.segment
    FROM metrics src
    LEFT JOIN customers p
        ON src.customer_id = p.id
    WHERE p.age BETWEEN 46 AND 61
),
with_dims_orders AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.customer_id,
        a.store_id,
        a.shipping_fee,
        a.discount_plus_amount,
        a.next_discount_plus_amount,
        a.segment,
        d.region,
        d.opened_date,
        d.sqft
    FROM with_dims a
    INNER JOIN stores d
        ON a.store_id = d.id
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN sqft >= 15465 THEN 'HIGH' ELSE 'LOW' END AS sqft_tier,
        APPROX_PERCENTILE(discount_plus_amount, 0.5) AS discount_plus_amount_p50,
        COUNT(CASE WHEN segment = 'Corporate' THEN 1 END) AS corporate_count,
        COUNT_IF(region = 'West') AS west_events,
        SUM(CASE WHEN segment = 'SMB' THEN shipping_fee ELSE 0 END) AS smb_shipping_fee,
        APPROX_PERCENTILE(discount_plus_amount, ARRAY(0.25, 0.5, 0.75)) AS discount_plus_amount_quartiles,
        SIZE(COLLECT_SET(region)) AS num_regions
    FROM with_dims_orders
    GROUP BY CASE WHEN sqft >= 15465 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    sqft_tier,
    discount_plus_amount_p50,
    corporate_count,
    discount_plus_amount_quartiles,
    num_regions
FROM totals
