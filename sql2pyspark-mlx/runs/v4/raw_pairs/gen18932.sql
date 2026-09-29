SELECT
    id,
    supplier_name,
    city,
    rating,
    CASE WHEN country = 'US' THEN 1 ELSE 0 END AS is_us
FROM suppliers;
