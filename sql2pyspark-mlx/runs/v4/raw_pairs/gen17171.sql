SELECT
    id,
    country,
    age,
    segment,
    IF(is_active, 'Yes', 'No') AS is_active_label
FROM customers;
