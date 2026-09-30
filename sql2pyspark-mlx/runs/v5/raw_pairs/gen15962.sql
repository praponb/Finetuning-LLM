SELECT location, id FROM departments EXCEPT SELECT location, id FROM departments WHERE location IN ('New York', 'London', 'Bangkok');
