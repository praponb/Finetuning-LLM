SELECT CASE WHEN dept_name IN ('Engineering', 'Finance') THEN 'priority' ELSE 'other' END dept_name_group, SUM(CASE WHEN location = 'Berlin' THEN 1 ELSE 0 END) berlin_count, COUNT(*) total_count
FROM departments
GROUP BY 1
ORDER BY dept_name_group DESC;
