SELECT
    id,
    email,
    CONCAT_WS('-', email, job_title) email_job_title_key,
    LAST_DAY(hire_date) hire_date_month_end
FROM employees
WHERE level NOT IN ('L2', 'L4');
