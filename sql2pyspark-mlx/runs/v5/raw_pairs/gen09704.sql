SELECT
    i.last_restock,
    i.store_id,
    s.sqft,
    s.store_name,
    p.supplier_id,
    p.stock_qty,
    CONCAT_WS(',', s.store_name, s.city) AS store_name_city_key
FROM inventory i
JOIN stores s ON i.store_id = s.id
LEFT OUTER JOIN products p ON i.product_id = p.id
WHERE s.sqft BETWEEN 4505 AND 13332
