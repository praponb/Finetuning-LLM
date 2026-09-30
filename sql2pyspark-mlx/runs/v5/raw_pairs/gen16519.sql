SELECT
    manager_id,
    hire_date,
    is_remote,
    CAST(is_remote AS INT) AS is_remote_flag
FROM employees
