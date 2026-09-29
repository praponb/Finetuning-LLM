SELECT
    name,
    age,
    country,
    CAST(is_active AS INT) AS is_active_flag
FROM customers;
