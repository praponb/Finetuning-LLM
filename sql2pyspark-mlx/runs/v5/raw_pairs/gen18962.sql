SELECT id, manager_id, level, SUBSTRING(full_name, 1, 4) full_name_prefix FROM employees ORDER BY id DESC
