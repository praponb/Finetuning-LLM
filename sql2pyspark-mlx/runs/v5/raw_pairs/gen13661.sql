SELECT
    i.id,
    i.product_id,
    i.store_id,
    s.store_name,
    s.city,
    CAST(i.quantity AS DOUBLE) AS quantity_num
FROM inventory i
FULL OUTER JOIN stores s ON i.store_id = s.id
