SELECT *
FROM departments
WHERE budget IS NOT NULL
ORDER BY budget ASC, location ASC;
