WITH base AS (
    SELECT *
    FROM customers
    WHERE email LIKE '%@GMAIL.COM' OR country = 'DE'
),
summary AS (
    SELECT CASE WHEN segment IN ('Consumer', 'SMB') THEN 'group_a' ELSE 'other' END AS segment_group, segment, AVG(age) AS avg_age
    FROM base
    GROUP BY CASE WHEN segment IN ('Consumer', 'SMB') THEN 'group_a' ELSE 'other' END, segment
)
SELECT segment_group, segment, avg_age
FROM summary
ORDER BY segment_group, segment
LIMIT 5;
