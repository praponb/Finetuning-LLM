SELECT location, pct_berlin
FROM (
    SELECT location, ROUND((100 * SUM(CASE WHEN location = 'Berlin' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_berlin
    FROM departments
    GROUP BY location
) AS s
WHERE pct_berlin < 42;
