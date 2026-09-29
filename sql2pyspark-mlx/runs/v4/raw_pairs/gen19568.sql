SELECT *
FROM (
    SELECT
        dept_name,
        location,
        budget
    FROM departments
) src
PIVOT (
    AVG(budget) AS value_sum,
    COUNT(1) AS cnt
    FOR location IN ('Berlin' AS berlin, 'Bangkok' AS bangkok)
);
