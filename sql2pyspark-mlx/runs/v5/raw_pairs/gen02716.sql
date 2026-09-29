SELECT
    email,
    salary,
    TRIM(level) AS clean_level,
    DATE_SUB(hire_date, 75) AS hire_date_minus_75d
FROM employees
WHERE salary >= 93000 AND full_name LIKE 'G%';
