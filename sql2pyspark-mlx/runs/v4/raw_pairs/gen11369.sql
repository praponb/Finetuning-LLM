WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY cost, id) AS rank_in_group
        FROM products
    ) ranked
    WHERE rank_in_group = 1
)
SELECT
    s.id,
    s.category,
    s.cost,
    s.stock_qty,
    s.launch_date,
    s.tags,
    s.supplier_id,
    (SELECT MIN(p2.cost) FROM products p2 WHERE p2.supplier_id = s.supplier_id) AS min_product_cost
FROM ranked s
ORDER BY supplier_id, cost
