WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share,
        SUM(sqft) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft,
        AVG(sqft) OVER (ORDER BY sqft, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS sqft_avg_3
    FROM stores
    WHERE city <> 'Bangkok'
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.city,
        s.opened_date,
        s.sqft_share,
        s.sqft_avg_3,
        top1.discount,
        top1.shipping_fee
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.discount, c.shipping_fee
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, LEAST(sqft_share, discount) AS least_sqft_share_discount
    FROM with_last
    WHERE city <> 'Chiang Mai'
),
with_last_stores AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.city,
        s.sqft_share,
        s.shipping_fee,
        s.least_sqft_share_discount,
        top1.inventory_quantity
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Grouped metrics
SELECT
    CASE WHEN inventory_quantity >= 214 THEN 'HIGH' ELSE 'LOW' END AS inventory_quantity_tier,
    APPROX_PERCENTILE(least_sqft_share_discount, 0.9) AS least_sqft_share_discount_p90,
    SUM(least_sqft_share_discount) FILTER (WHERE city = 'Chiang Mai') AS chiang_mai_total_least_sqft_share_discount,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_last_stores
GROUP BY CASE WHEN inventory_quantity >= 214 THEN 'HIGH' ELSE 'LOW' END
ORDER BY inventory_quantity_tier NULLS FIRST;
