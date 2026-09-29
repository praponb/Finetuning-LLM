WITH long_format AS (
    SELECT *
    FROM (
        SELECT
            id,
            brand,
            cost,
            price
        FROM products
    ) src
    UNPIVOT (
        val FOR metric IN (cost, price)
    ) long_rows
)
SELECT
    CASE WHEN val >= 68 THEN 'HIGH' ELSE 'LOW' END AS val_tier,
    ROUND(AVG(val), 2) AS avg_val,
    ROUND(SUM(val) / NULLIF(COUNT(CASE WHEN metric = 'price' THEN 1 END), 0), 2) AS val_per_price
FROM long_format
GROUP BY CASE WHEN val >= 68 THEN 'HIGH' ELSE 'LOW' END
HAVING ROUND(SUM(val) / NULLIF(COUNT(CASE WHEN metric = 'price' THEN 1 END), 0), 2) < 1000000
ORDER BY val_tier DESC;
