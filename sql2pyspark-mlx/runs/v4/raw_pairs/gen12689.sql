SELECT d.dept_name, t.row_count, t.l1_salary
FROM departments d
LEFT JOIN (SELECT dept_id, COUNT(*) AS row_count, SUM(CASE WHEN level = 'L1' THEN salary ELSE 0 END) AS l1_salary
    FROM employees
    GROUP BY dept_id) t ON d.id = t.dept_id
ORDER BY t.l1_salary DESC, row_count DESC;
