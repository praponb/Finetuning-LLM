SELECT
    age,
    email,
    name,
    IF(is_active, 'Yes', 'No') AS is_active_label
FROM customers
WHERE credit_limit <= 15600;
