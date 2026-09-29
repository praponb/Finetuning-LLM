SELECT
    level,
    email,
    job_title,
    CAST(is_remote AS INT) AS is_remote_flag
FROM employees
WHERE salary BETWEEN 103000 AND 143000 OR is_remote = TRUE
ORDER BY is_remote_flag ASC
