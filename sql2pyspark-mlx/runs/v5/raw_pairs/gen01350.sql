select id, location, budget, concat(dept_name, '/', location) as dept_name_location from departments where location != 'Bangkok' and location = 'Chiang Mai' and budget <= 3393000;
