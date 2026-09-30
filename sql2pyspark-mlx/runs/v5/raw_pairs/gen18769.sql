SELECT id, dept_id, TRIM(full_name) AS clean_full_name, CASE WHEN level IN ('L3', 'L2') THEN 'Core' ELSE 'Other' END AS level_group FROM employees WHERE is_remote = FALSE
