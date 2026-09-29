SELECT
    is_active,
    age,
    credit_limit - age AS credit_limit_minus_age,
    IF(is_active, 'Yes', 'No') AS is_active_label
FROM customers;
