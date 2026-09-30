SELECT
    src.*,
    s.rating,
    s.city
FROM products src
INNER JOIN suppliers s
    ON src.supplier_id = s.id;
