SELECT
    id,
    level,
    full_name,
    IF(is_remote, 'Yes', 'No') AS is_remote_label
FROM employees
WHERE salary > 89000
ORDER BY full_name DESC, is_remote_label DESC;
