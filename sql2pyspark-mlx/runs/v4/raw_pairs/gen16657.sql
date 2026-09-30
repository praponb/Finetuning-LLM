SELECT location, id FROM departments INTERSECT SELECT location, id FROM departments WHERE budget <= 1320000;
