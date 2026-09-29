SELECT
    id,
    is_active,
    email,
    SUBSTRING(name, 1, 4) name_prefix
FROM customers
ORDER BY name_prefix, email, id
LIMIT 5;
