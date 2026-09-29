WITH kept_keys AS (
    SELECT supplier_id, id
    FROM products
    EXCEPT
    SELECT supplier_id, id
    FROM products
    WHERE supplier_id IS NOT NULL
),
verified AS (
    SELECT s.*
    FROM products s
    INNER JOIN kept_keys k ON s.supplier_id = k.supplier_id AND s.id = k.id
)
SELECT
    CASE WHEN price >= 1200 THEN 'LARGE' ELSE 'SMALL' END AS price_tier,
    supplier_id,
    AVG(cost) FILTER (WHERE category = 'Sports') AS sports_avg_cost,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(stock_qty), 2) AS avg_stock_qty,
    COUNT_IF(brand = 'Acme') AS acme_events,
    RANK() OVER (PARTITION BY CASE WHEN price >= 1200 THEN 'LARGE' ELSE 'SMALL' END ORDER BY ROUND(AVG(stock_qty), 2) DESC) AS avg_stock_qty_rank
FROM verified
GROUP BY CASE WHEN price >= 1200 THEN 'LARGE' ELSE 'SMALL' END, supplier_id
ORDER BY price_tier DESC NULLS LAST;
