SELECT
    id,
    hire_date,
    email,
    CASE WHEN level = 'L1' THEN 1 ELSE 0 END is_l1
FROM employees;
