SELECT *
FROM (
    SELECT
        id,
        dept_name,
        budget,
        location,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) AS row_num
    FROM departments
) AS t
WHERE row_num <= 3;
