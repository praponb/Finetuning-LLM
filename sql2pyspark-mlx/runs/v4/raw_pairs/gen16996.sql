WITH filtered AS (
    SELECT
        *,
        CASE WHEN category IN ('Home', 'Sports') THEN 'Group A' ELSE 'Other' END AS category_group,
        ROUND(price, 1) AS price_rounded
    FROM products
    WHERE launch_date >= TO_DATE('2023-12-06')
),
metrics AS (
    SELECT
        *,
        CORR(stock_qty, cost) OVER (PARTITION BY brand) AS corr_stock_qty_cost,
        LAST_VALUE(price_rounded) IGNORE NULLS OVER (PARTITION BY brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_price_rounded
    FROM filtered
),
not_flagged AS (
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.quantity >= 5
        )
),
enriched AS (
    SELECT
        src.*,
        p.city,
        p.rating
    FROM not_flagged src
    LEFT JOIN suppliers p
        ON src.supplier_id = p.id
)
SELECT
    CASE WHEN cost >= 720 THEN 'HIGH' ELSE 'LOW' END AS cost_tier,
    CORR(stock_qty, cost) AS corr_stock_qty_cost,
    COUNT(*) AS num_rows
FROM enriched
GROUP BY CASE WHEN cost >= 720 THEN 'HIGH' ELSE 'LOW' END
ORDER BY corr_stock_qty_cost DESC NULLS LAST;
