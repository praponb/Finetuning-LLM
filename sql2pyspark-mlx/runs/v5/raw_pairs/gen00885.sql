SELECT id, full_name, CASE WHEN level = 'L4' THEN 1 ELSE 0 END AS is_l4
FROM employees
WHERE (full_name LIKE 'J%' AND level NOT IN ('L2', 'L3', 'L1')) OR full_name LIKE 'b%'
