SELECT
    id,
    job_title,
    full_name,
    CAST(is_remote AS INT) AS is_remote_flag
FROM employees
WHERE NOT (level = 'L5' OR LENGTH(full_name) > 11)
ORDER BY full_name DESC, id
LIMIT 20;
