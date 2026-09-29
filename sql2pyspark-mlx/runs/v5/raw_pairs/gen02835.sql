SELECT
    id,
    email,
    dept_id,
    salary,
    ADD_MONTHS(hire_date, 1) hire_date_plus_months
FROM employees
WHERE email LIKE '%@company.co' AND full_name IS NOT NULL
