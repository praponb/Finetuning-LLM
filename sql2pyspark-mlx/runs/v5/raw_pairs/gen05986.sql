SELECT id, dept_name, location, CASE WHEN dept_name = 'Finance' THEN 1 ELSE 0 END AS is_finance FROM departments WHERE location = 'Berlin' AND budget >= 3064000 AND budget > 2258000;
