SELECT
    id,
    email,
    full_name,
    LAST_DAY(hire_date) hire_date_month_end
FROM employees
WHERE salary >= 125000
ORDER BY email;
