WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(store_name)) AS store_name_title,
        COALESCE(region, 'N/A') AS region_clean
    FROM stores
    WHERE region IS NOT NULL
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.order_date
    FROM filtered s
    JOIN LATERAL (
            SELECT c.order_date
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.last_restock
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share,
        SUM(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
        LEAD(sqft, 1) OVER (PARTITION BY city, region ORDER BY last_restock, id) AS next_sqft,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC, id) AS row_seq
    FROM with_latest
),
with_latest_stores AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.quantity,
        latest.inventory_last_restock
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.quantity, c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_latest_stores
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft NULLS FIRST, id) = 1
),
latest_stores AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC NULLS LAST, id) AS rank_in_group
    FROM latest
    QUALIFY rank_in_group = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN sqft_share >= 0.67 THEN 'HIGH' ELSE 'LOW' END AS sqft_share_tier,
    COUNT_IF(region = 'Central') AS central_events,
    SUM(CASE WHEN region = 'North' THEN sqft ELSE 0 END) AS north_sqft,
    COUNT(*) AS event_count,
    ROUND(AVG(next_sqft), 2) AS avg_next_sqft,
    SUM(CASE WHEN city = 'London' THEN next_sqft ELSE 0 END) AS london_next_sqft
FROM latest_stores
GROUP BY CASE WHEN sqft_share >= 0.67 THEN 'HIGH' ELSE 'LOW' END
HAVING london_next_sqft >= 0
ORDER BY north_sqft DESC NULLS FIRST
