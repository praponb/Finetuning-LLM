WITH order_items_by_product AS (
    SELECT product_id, COUNT(*) AS row_count
    FROM order_items
    GROUP BY product_id
)
SELECT
    p.id,
    p.stock_qty,
    p.supplier_id,
    oibp.row_count
FROM products p
JOIN order_items_by_product oibp ON p.id = oibp.product_id
WHERE oibp.row_count > 6
ORDER BY row_count DESC
