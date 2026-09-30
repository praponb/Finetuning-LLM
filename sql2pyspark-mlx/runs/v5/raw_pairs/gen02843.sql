SELECT
    suppliers.country,
    suppliers.rating,
    products.cost,
    products.price,
    products.category
FROM suppliers
LEFT OUTER JOIN products ON products.supplier_id = suppliers.id
