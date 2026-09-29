SELECT
    *,
    NTILE(10) OVER w_ordered quantity_bucket,
    ROW_NUMBER() OVER (ORDER BY unit_price, id) event_seq,
    NTILE(3) OVER w_ordered quantity_bucket_2
FROM order_items
WINDOW w_ordered AS (PARTITION BY product_id ORDER BY quantity, id);
