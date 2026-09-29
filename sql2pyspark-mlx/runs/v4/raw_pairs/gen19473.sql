WITH cleaned AS (
    SELECT *, CASE WHEN location = 'New York' THEN 1 ELSE 0 END AS is_new_york
    FROM departments
    WHERE dept_name IS NOT NULL
)
SELECT *
FROM (
    SELECT
        dept_name,
        location,
        budget
    FROM cleaned
) src
PIVOT (
    MAX(budget) AS amount,
    COUNT(1) AS item_count
    FOR location IN ('New York' AS new_york, 'Singapore' AS singapore)
);
