SELECT
    id,
    location,
    budget
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget ASC, id ASC) AS latest_rank
    FROM departments
) ranked
WHERE latest_rank = 1
