WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY discount DESC) AS rn
    FROM orders
    QUALIFY rn = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        store_id,
        status,
        amount,
        shipping_fee,
        rn
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY amount DESC NULLS FIRST, id) = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        status,
        amount,
        shipping_fee,
        rn,
        ROUND(shipping_fee * 1.1, 2) AS adjusted_shipping_fee
    FROM deduped
    WHERE store_id IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.order_id = s.id
              AND x.product_id = 1
        )
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        e.*,
        b.region,
        b.opened_date,
        b.store_name
    FROM not_flagged e
    LEFT JOIN stores b
        ON e.store_id <=> b.id
    WHERE b.opened_date < '2025-03-01'
),
matched_keys AS (
    -- Set difference on keys
    SELECT store_id
    FROM with_dims
    EXCEPT ALL
    SELECT store_id
    FROM orders
    WHERE discount BETWEEN 0.2 AND 0.28
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    JOIN matched_keys k ON s.store_id = k.store_id
),
with_dims_orders AS (
    -- Attach stores attributes
    SELECT
        a.*,
        p.opened_date AS store_opened_date,
        p.sqft,
        p.store_name AS store_store_name
    FROM reconciled a
    JOIN stores p
        ON a.store_id = p.id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    status,
    CASE WHEN shipping_fee >= 40 THEN 'HIGH' ELSE 'LOW' END AS shipping_fee_tier,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    SIZE(COLLECT_SET(region)) AS num_regions
FROM with_dims_orders
GROUP BY DATE_TRUNC('MONTH', opened_date), status, CASE WHEN shipping_fee >= 40 THEN 'HIGH' ELSE 'LOW' END
