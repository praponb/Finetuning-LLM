SELECT x.city, y.job_title FROM (SELECT DISTINCT city FROM customers) x CROSS JOIN (SELECT DISTINCT job_title FROM employees) y
