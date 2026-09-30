SELECT
    *,
    LAG(stock_qty, 1) OVER (ORDER BY launch_date ASC, id ASC) AS prev_stock_qty,
    ROW_NUMBER() OVER w_hist AS event_seq,
    ROW_NUMBER() OVER (PARTITION BY category, brand ORDER BY launch_date DESC, id ASC) AS event_seq_2
FROM products
WHERE supplier_id IN (3, 6, 23)
WINDOW w_hist AS (PARTITION BY brand ORDER BY stock_qty DESC, id ASC);
