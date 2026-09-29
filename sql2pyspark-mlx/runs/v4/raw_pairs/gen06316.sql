SELECT id, level, full_name FROM employees WHERE is_remote = FALSE OR level IN ('L1', 'L2');
