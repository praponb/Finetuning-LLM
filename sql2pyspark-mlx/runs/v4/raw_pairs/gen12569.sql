WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS rnk
    FROM departments
)
SELECT *
FROM ranked
WHERE rnk <= 3
ORDER BY id DESC, rnk DESC
