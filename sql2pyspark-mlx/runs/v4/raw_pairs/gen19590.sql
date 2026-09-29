SELECT
    dept_id,
    manager_id,
    is_remote,
    DATE_ADD(hire_date, 51) hire_date_plus_51d
FROM employees
WHERE is_remote OR is_remote;
