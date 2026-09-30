WITH filtered AS (
    SELECT
        id,
        store_name,
        city,
        sqft,
        CASE WHEN sqft >= 15462 THEN 'High' WHEN sqft >= 4981 THEN 'Medium' ELSE 'Low' END AS sqft_level,
        COALESCE(region, 'N/A') AS resolved_region
    FROM stores
    WHERE opened_date >= TO_DATE('2024-02-06')
      AND region IS NOT NULL
      AND opened_date BETWEEN '2024-09-14' AND '2025-03-01'
),
with_latest AS (
    SELECT
        s.*,
        top1.order_shipping_fee,
        top1.status
    FROM filtered s
    JOIN LATERAL (
            SELECT c.shipping_fee AS order_shipping_fee, c.status
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last AS (
    SELECT
        s.*,
        top1.last_restock,
        top1.quantity
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.last_restock, c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    SELECT
        id,
        store_name,
        city,
        sqft,
        sqft_level,
        resolved_region,
        order_shipping_fee,
        status,
        last_restock,
        quantity,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY quantity DESC NULLS FIRST) AS quantity_dense_rank,
        NTILE(5) OVER (ORDER BY quantity, id) AS quantity_bucket
    FROM with_last
)
SELECT
    CASE WHEN quantity_bucket >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_bucket_tier,
    city,
    CASE WHEN quantity_dense_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END AS quantity_dense_rank_tier,
    SUM(CASE WHEN city = 'London' THEN order_shipping_fee ELSE 0 END) AS london_order_shipping_fee,
    SUM(sqft) FILTER (WHERE status = 'RETURNED') AS returned_total_sqft,
    GROUPING_ID() AS grouping_id
FROM enriched
GROUP BY GROUPING SETS ((CASE WHEN quantity_bucket >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, city, CASE WHEN quantity_dense_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN quantity_bucket >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, city), (CASE WHEN quantity_bucket >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END))
HAVING returned_total_sqft < 0;
