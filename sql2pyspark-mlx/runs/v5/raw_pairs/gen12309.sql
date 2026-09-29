WITH valid_stores AS (
    SELECT *, ROUND(sqft * 0.9, 2) AS sqft_adjusted
    FROM stores
    WHERE city IN ('Tokyo', 'New York')
),
summary AS (
    SELECT MONTH(opened_date) AS opened_month, region, AVG(sqft) AS avg_sqft
    FROM valid_stores
    GROUP BY MONTH(opened_date), region
)
SELECT opened_month, region, avg_sqft
FROM summary
WHERE avg_sqft < 13200;
