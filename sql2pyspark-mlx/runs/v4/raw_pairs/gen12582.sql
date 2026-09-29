SELECT
    o.discount,
    o.status,
    s.opened_date,
    s.store_name,
    s.sqft
FROM orders o
INNER JOIN stores s ON o.store_id = s.id;
