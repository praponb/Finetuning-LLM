WITH with_last AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.sale_date,
        top1.store_sale_revenue
    FROM products s
    LEFT JOIN LATERAL (
            SELECT c.sale_date, c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Multi-level aggregation
SELECT
    CASE WHEN price >= 1250 THEN 'HIGH' ELSE 'LOW' END AS price_tier,
    supplier_id,
    brand,
    CORR(cost, price) AS corr_cost_price,
    PERCENTILE_APPROX(stock_qty, 0.5) AS stock_qty_p50,
    SUM(CASE WHEN brand = 'Acme' THEN stock_qty ELSE 0 END) AS acme_stock_qty,
    SIZE(COLLECT_SET(category)) AS num_categories,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN price >= 1250 THEN 'HIGH' ELSE 'LOW' END ORDER BY CORR(cost, price) DESC) AS corr_cost_price_dense_rank
FROM with_last
GROUP BY price_tier, supplier_id, brand
