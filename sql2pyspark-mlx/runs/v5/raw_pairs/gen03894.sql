SELECT ROUND((100 * SUM(CASE WHEN dept_name = 'Finance' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_finance FROM departments ORDER BY pct_finance;
