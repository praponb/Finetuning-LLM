SELECT
    id,
    unit_price,
    order_id,
    quantity,
    ROW_NUMBER() OVER (ORDER BY unit_price, id) AS row_num,
    RANK() OVER (PARTITION BY order_id ORDER BY quantity DESC) AS quantity_rank
FROM order_items
WHERE quantity >= 6;
