WITH valid_customers AS (
    SELECT *
    FROM customers
    WHERE country = 'JP'
),
Agg AS (
    SELECT country, ROUND((100 * SUM(CASE WHEN segment = 'SMB' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_smb, COUNT(CASE WHEN segment = 'Consumer' THEN 1 END) AS consumer_count
    FROM valid_customers
    GROUP BY country
)
SELECT country, consumer_count
FROM Agg
WHERE consumer_count < 6
ORDER BY 1
