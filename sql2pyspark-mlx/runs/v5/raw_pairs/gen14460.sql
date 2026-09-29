SELECT
    id,
    hire_date,
    CASE WHEN level = 'L2' THEN 1 ELSE 0 END AS is_l2,
    CONCAT_WS('-', email, full_name) AS email_full_name_key
FROM employees
WHERE hire_date <= '2024-08-19';
