SELECT
    *,
    PERCENT_RANK() OVER (ORDER BY stock_qty DESC NULLS FIRST) stock_qty_percent_rank,
    ROUND(price / SUM(price) OVER (PARTITION BY supplier_id), 4) price_share,
    ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY cost, id) row_seq
FROM products
WHERE cost BETWEEN 520 AND 620
