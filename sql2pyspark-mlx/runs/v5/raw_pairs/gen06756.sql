SELECT
    id,
    product_id,
    unit_price,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price DESC, id) AS rn
FROM order_items;
