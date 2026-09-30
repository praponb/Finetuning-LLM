SELECT
    id,
    salary,
    is_remote,
    ADD_MONTHS(hire_date, 3) AS hire_date_plus_months
FROM employees
WHERE hire_date BETWEEN '2024-10-01' AND '2024-11-01' AND manager_id = 21 AND is_remote;
