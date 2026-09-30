SELECT location
FROM departments
EXCEPT ALL
SELECT location
FROM departments
WHERE budget <= 2916000;
