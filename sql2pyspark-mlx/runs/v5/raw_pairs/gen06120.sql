WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC NULLS FIRST, id) AS dedup_rank
    FROM inventory
    QUALIFY dedup_rank = 1
),
metrics AS (
    SELECT
        *,
        CUME_DIST() OVER w_recent AS quantity_cume_dist,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity
    FROM latest
    WINDOW w_recent AS (PARTITION BY store_id ORDER BY quantity DESC)
),
remaining_keys AS (
    SELECT product_id, id
    FROM metrics
    INTERSECT
    SELECT product_id, id
    FROM inventory
    WHERE product_id IN (5, 8)
),
reconciled AS (
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id AND s.id = k.id
),
enriched AS (
    SELECT
        a.*,
        p.opened_date,
        p.sqft
    FROM reconciled a
    INNER JOIN stores p
        ON a.store_id = p.id
)
SELECT
    p.category,
    COUNT(*) AS cnt,
    ROUND(AVG(a.sqft), 2) AS avg_sqft,
    ROUND(SUM(a.quantity), 2) AS total_quantity,
    GROUPING_ID() AS level_id
FROM enriched a
LEFT JOIN products p ON a.product_id = p.id
GROUP BY CUBE (p.category)
ORDER BY avg_sqft DESC NULLS FIRST;
