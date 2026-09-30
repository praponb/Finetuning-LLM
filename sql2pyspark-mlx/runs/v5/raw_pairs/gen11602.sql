WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS pos
    FROM departments
    WHERE dept_name = 'Legal'
)
SELECT *
FROM numbered
WHERE pos = 1
