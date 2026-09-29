SELECT location, dept_name FROM departments WHERE location = 'Berlin' INTERSECT SELECT location, dept_name FROM departments WHERE budget <= 1605000
