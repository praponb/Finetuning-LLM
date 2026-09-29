SELECT id, location
FROM departments
WHERE budget > 3644000
INTERSECT
SELECT id, location
FROM departments
WHERE location = 'London';
