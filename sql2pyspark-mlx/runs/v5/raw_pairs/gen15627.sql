SELECT
    *,
    AVG(quantity) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS quantity_avg_2,
    ROW_NUMBER() OVER (ORDER BY quantity, id) AS seq_no,
    CUME_DIST() OVER (PARTITION BY product_id, order_id ORDER BY quantity DESC NULLS FIRST) AS quantity_cume_dist
FROM order_items
