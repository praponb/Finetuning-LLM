SELECT
    id,
    rating,
    TRIM(supplier_name) AS clean_supplier_name,
    ROUND(rating * 100, 2) AS rating_adjusted
FROM suppliers
ORDER BY rating ASC, clean_supplier_name ASC;
