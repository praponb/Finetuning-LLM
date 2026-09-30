WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        sqft,
        CONCAT(store_name, ' ', city) AS store_name_city
    FROM stores
    WHERE opened_date >= DATE '2023-06-03'
      AND region IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        city,
        sqft,
        FIRST_VALUE(sqft) IGNORE NULLS OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft,
        LEAD(sqft, 2) OVER (PARTITION BY city ORDER BY sqft, id) AS next_sqft,
        SUM(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft,
        LAST_VALUE(sqft, TRUE) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_sqft,
        LAST_VALUE(sqft, TRUE) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_sqft_2
    FROM cleaned
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.running_sqft,
        s.last_known_sqft,
        latest.last_restock,
        latest.quantity
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.last_restock, c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics_stores AS (
    -- Running and moving aggregates
    SELECT
        id,
        last_known_sqft,
        last_restock,
        quantity,
        NTILE(4) OVER (PARTITION BY city ORDER BY quantity, id) AS quantity_bucket,
        ROUND(running_sqft / SUM(running_sqft) OVER (PARTITION BY city), 4) AS running_sqft_share,
        SUM(last_known_sqft) OVER (PARTITION BY city ORDER BY last_known_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last_known_sqft,
        MIN(running_sqft) OVER (PARTITION BY city) AS city_min_running_sqft
    FROM latest_match
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics_stores s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.customer_id IN (21, 35)
        )
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.inventory_last_restock,
        x.inventory_quantity
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock, c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.store_id IN (1, 2)
        )
)
-- Grouped metrics
SELECT
    CASE WHEN running_sqft_share >= 0.67 THEN 'LARGE' ELSE 'SMALL' END AS running_sqft_share_tier,
    DATE_TRUNC('MONTH', last_restock) AS activity_month,
    COUNT(*) AS row_count,
    ROUND(AVG(inventory_quantity), 2) AS avg_inventory_quantity,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS row_count_2
FROM not_flagged
GROUP BY CASE WHEN running_sqft_share >= 0.67 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', last_restock)
ORDER BY running_sqft_share_tier DESC;
