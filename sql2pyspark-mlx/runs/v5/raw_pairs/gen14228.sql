SELECT budget, COALESCE(dept_name, 'N/A') AS dept_name_filled FROM departments WHERE location IN ('Chiang Mai', 'London', 'Berlin') AND location IN ('Tokyo', 'New York');
