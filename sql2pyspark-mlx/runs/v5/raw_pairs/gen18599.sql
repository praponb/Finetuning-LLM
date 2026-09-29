SELECT dept_name, location, SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list FROM departments WHERE location = 'Bangkok' AND budget > 1565000 GROUP BY dept_name, location;
