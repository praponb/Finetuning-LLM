SELECT
    id,
    city,
    SUBSTRING(supplier_name, 1, 4) AS supplier_name_prefix,
    ROUND(rating * 0.85, 2) AS rating_scaled
FROM suppliers
WHERE rating <= 1.8 AND country <> 'TH';
