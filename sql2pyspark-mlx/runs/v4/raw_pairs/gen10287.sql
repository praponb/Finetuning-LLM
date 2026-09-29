SELECT
    *,
    ROUND(quantity / SUM(quantity) OVER (PARTITION BY product_id), 4) AS quantity_share,
    ROW_NUMBER() OVER (ORDER BY quantity DESC, id) AS event_seq
FROM order_items;
