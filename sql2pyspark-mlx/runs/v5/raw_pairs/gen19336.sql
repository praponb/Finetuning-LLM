SELECT id, level, COUNT(*) OVER (PARTITION BY level) AS level_count FROM employees WHERE LENGTH(full_name) > 5;
