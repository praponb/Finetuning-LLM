WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        CAST(revenue AS INT) AS revenue_int
    FROM store_sales
    WHERE sale_date > DATE '2023-08-12'
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        s.*,
        p.city,
        p.region,
        p.sqft
    FROM filtered s
    INNER JOIN stores p
        ON s.store_id = p.id
),
enriched AS (
    -- Attach stores attributes
    SELECT
        s.*,
        p.opened_date,
        p.sqft AS store_sqft
    FROM with_dims s
    INNER JOIN stores p
        ON s.store_id = p.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(revenue) OVER (PARTITION BY city ORDER BY store_sqft, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS revenue_avg_6,
        RANK() OVER w_grp AS revenue_int_rank,
        SUM(revenue) OVER w_running AS running_revenue
    FROM enriched
    WINDOW w_grp AS (PARTITION BY store_id ORDER BY revenue_int), w_running AS (ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    s.id,
    s.product_id,
    s.sale_date,
    s.units,
    s.revenue,
    s.region,
    s.revenue_int_rank,
    s.running_revenue,
    CASE WHEN region RLIKE '^(South|East)$' THEN 'GROUP_B' WHEN region RLIKE '^(North|West)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS region_zone,
    (SELECT AVG(t2.quantity) FROM inventory t2 WHERE t2.product_id = s.product_id) AS avg_inventory_quantity
FROM with_windows s
ORDER BY units DESC, id;
