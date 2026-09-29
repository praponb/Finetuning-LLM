SELECT SUM(CASE WHEN location = 'London' THEN 1 ELSE 0 END) AS london_count, COUNT(DISTINCT budget) AS unique_dept_names FROM departments WHERE budget IS NULL;
