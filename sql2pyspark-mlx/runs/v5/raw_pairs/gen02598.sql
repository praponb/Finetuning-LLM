SELECT dept_name, hr_count
FROM (
    SELECT dept_name, COUNT(CASE WHEN dept_name = 'HR' THEN 1 END) AS hr_count
    FROM departments
    WHERE location IN ('Berlin', 'London')
    GROUP BY dept_name
) AS x
WHERE hr_count < 8
