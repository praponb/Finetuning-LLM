SELECT
    *,
    LAG(unit_price) OVER (PARTITION BY product_id, order_id ORDER BY quantity ASC, id ASC) AS prev_unit_price,
    LEAD(unit_price) OVER (PARTITION BY product_id, order_id ORDER BY quantity ASC, id ASC) AS next_unit_price,
    LAG(quantity) OVER (PARTITION BY product_id, order_id ORDER BY quantity ASC, id ASC) AS prev_quantity,
    LEAD(quantity) OVER (PARTITION BY product_id, order_id ORDER BY quantity ASC, id ASC) AS next_quantity,
    ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY quantity ASC, id ASC) AS seq_no,
    CORR(unit_price, quantity) OVER (PARTITION BY order_id) AS corr_unit_price_quantity
FROM order_items;
