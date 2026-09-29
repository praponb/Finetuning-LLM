SELECT
    id,
    manager_id,
    level,
    dept_id,
    SPLIT(LOWER(email), '@')[0] AS email_user
FROM employees
WHERE NOT (full_name IS NULL AND is_remote = FALSE)
ORDER BY dept_id, email_user
