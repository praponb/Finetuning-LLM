SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) AS row_seq,
    CORR(quantity, unit_price) OVER (PARTITION BY product_id) AS corr_quantity_unit_price,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS seq_no,
    AVG(unit_price) OVER (ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS unit_price_avg_2_prior
FROM order_items
