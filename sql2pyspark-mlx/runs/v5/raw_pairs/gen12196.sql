WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(brand, 1, 4) brand_prefix,
        DATEDIFF(CURRENT_DATE(), launch_date) days_since_launch_date,
        COALESCE(brand, 'OTHER') brand_clean
    FROM products
    WHERE product_name LIKE '%Offi%'
),
with_last AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name,
        s.category,
        s.brand,
        s.price,
        s.cost,
        s.tags,
        s.supplier_id,
        s.brand_clean,
        top1.sale_date,
        top1.revenue
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.sale_date, c.revenue
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.order_id IN (9, 17)
        )
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.revenue >= 3700
        )
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_name,
        category,
        price,
        tags,
        supplier_id,
        revenue,
        SUM(price) OVER (ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_price,
        LEAD(cost, 1) OVER (PARTITION BY supplier_id, category ORDER BY revenue, id) next_cost,
        NTILE(3) OVER (PARTITION BY supplier_id ORDER BY price DESC, id) price_bucket,
        LAST_VALUE(revenue) IGNORE NULLS OVER (PARTITION BY supplier_id, category ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_revenue
    FROM eligible
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT
        e.*,
        s.city,
        s.supplier_name
    FROM enriched e
    LEFT JOIN suppliers s
        ON e.supplier_id = s.id
    WHERE s.rating <= 3.3
)
-- Join a dimension and aggregate
SELECT
    s.country,
    src.category,
    COUNT(*) AS cnt,
    ROUND(SUM(src.next_cost), 2) AS total_next_cost,
    GROUPING_ID() AS gid
FROM with_dims src
LEFT JOIN suppliers s ON src.supplier_id = s.id
GROUP BY GROUPING SETS ((s.country, src.category), (s.country), ())
ORDER BY total_next_cost NULLS FIRST
