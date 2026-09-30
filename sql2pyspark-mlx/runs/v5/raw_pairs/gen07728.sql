WITH joined AS (
    -- Attach stores attributes
    SELECT
        e.*,
        d.opened_date,
        d.store_name
    FROM store_sales e
    LEFT JOIN stores d
        ON e.store_id = d.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY units DESC NULLS LAST, id) AS rank_in_group
    FROM joined
    QUALIFY rank_in_group = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        revenue,
        opened_date,
        store_name,
        rank_in_group,
        COALESCE(revenue, 0) AS revenue_filled
    FROM latest
    WHERE sale_date > DATE '2023-04-18'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        product_id,
        sale_date,
        revenue,
        rank_in_group,
        revenue_filled
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date, id) = 1
),
latest_store_sales AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue, id) AS dedup_rank
    FROM ranked
    QUALIFY dedup_rank = 1
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN revenue >= 3400 THEN 'LARGE' ELSE 'SMALL' END AS revenue_tier,
        product_id,
        COUNT(*) AS row_count,
        COUNT(DISTINCT product_id) AS distinct_product_ids,
        MIN(revenue_filled) AS min_revenue_filled,
        COUNT(*) AS cnt,
        AVG(revenue) AS avg_revenue,
        GROUPING(CASE WHEN revenue >= 3400 THEN 'LARGE' ELSE 'SMALL' END) AS grouping_level
    FROM latest_store_sales
    GROUP BY CUBE (CASE WHEN revenue >= 3400 THEN 'LARGE' ELSE 'SMALL' END, product_id)
)
SELECT
    s.product_id,
    s.distinct_product_ids,
    s.min_revenue_filled,
    s.avg_revenue,
    s.grouping_level,
    (SELECT AVG(o2.quantity) FROM order_items o2 WHERE o2.product_id = s.product_id) AS avg_order_item_quantity
FROM totals s;
