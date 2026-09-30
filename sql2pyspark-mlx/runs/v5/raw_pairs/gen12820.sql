SELECT a.country, b.job_title FROM (SELECT DISTINCT country FROM customers) a CROSS JOIN (SELECT DISTINCT job_title FROM employees) b ORDER BY country, job_title;
