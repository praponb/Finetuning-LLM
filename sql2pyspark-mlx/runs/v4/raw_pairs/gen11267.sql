SELECT id, budget, dept_name, location, CONCAT_WS(',', location, dept_name) AS location_dept_name_key, TRIM(location) AS clean_location FROM departments WHERE budget <= 2027000 AND budget >= 2061000
