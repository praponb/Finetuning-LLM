WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY stock_qty DESC NULLS FIRST, id) AS dedup_rank
    FROM products
    QUALIFY dedup_rank = 1
)
SELECT
    category,
    supplier_id,
    CORR(stock_qty, price) AS corr_stock_qty_price,
    CORR(price, stock_qty) AS corr_price_stock_qty,
    MIN(stock_qty) AS min_stock_qty
FROM latest
GROUP BY category, supplier_id
