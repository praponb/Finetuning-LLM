SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY cost, id) AS event_seq,
    NTILE(5) OVER w_recent AS price_bucket
FROM products
WINDOW w_recent AS (PARTITION BY supplier_id ORDER BY price, id);
