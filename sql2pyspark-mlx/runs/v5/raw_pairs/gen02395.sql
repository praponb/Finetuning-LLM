WITH products_by_supplier AS (
    SELECT supplier_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(brand))) AS brand_values, SORT_ARRAY(COLLECT_SET(category)) AS category_list
    FROM products
    GROUP BY supplier_id
)
SELECT
    suppliers.supplier_name,
    suppliers.country,
    products_by_supplier.brand_values,
    products_by_supplier.category_list
FROM suppliers
INNER JOIN products_by_supplier ON suppliers.id = products_by_supplier.supplier_id;
