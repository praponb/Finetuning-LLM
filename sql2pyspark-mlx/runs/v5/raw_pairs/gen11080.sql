SELECT
    id,
    country,
    segment,
    IF(is_active, 'Yes', 'No') AS is_active_label
FROM customers
WHERE name IS NOT NULL AND segment IN ('Consumer', 'SMB') AND city = 'Tokyo';
