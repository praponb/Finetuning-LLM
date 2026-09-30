SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS row_num
    FROM departments
) AS t
WHERE row_num <= 3
ORDER BY budget DESC
