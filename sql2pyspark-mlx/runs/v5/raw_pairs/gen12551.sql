WITH Numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) rnk
    FROM departments
)
SELECT *
FROM Numbered
WHERE rnk <= 3;
