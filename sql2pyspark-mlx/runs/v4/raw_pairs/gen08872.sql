WITH valid AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        revenue,
        ROUND(revenue / units, 2) AS revenue_per_unit,
        revenue + units AS revenue_per_unit_2
    FROM store_sales
    WHERE units >= 16
),
enriched AS (
    SELECT e.*, b.cost
    FROM valid e
    JOIN products b
        ON e.product_id = b.id
),
with_dims AS (
    SELECT
        s.*,
        b.stock_qty,
        b.category
    FROM enriched s
    LEFT JOIN products b
        ON s.product_id = b.id
)
SELECT
    category,
    COUNT(CASE WHEN category = 'Sports' THEN 1 END) AS sports_count,
    SIZE(COLLECT_SET(category)) AS num_categories,
    ROUND(AVG(cost), 2) AS avg_cost,
    COUNT(*) FILTER (WHERE category = 'Books') AS books_rows,
    COUNT(*) FILTER (WHERE category = 'Sports') AS sports_rows,
    SIZE(COLLECT_SET(category)) AS num_categories_2
FROM with_dims
GROUP BY category
ORDER BY sports_count DESC
