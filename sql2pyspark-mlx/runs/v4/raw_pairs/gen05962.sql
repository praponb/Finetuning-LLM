WITH valid AS (
    SELECT *, ROUND(units, 0) AS units_rounded
    FROM store_sales
    WHERE store_id IN (2, 4)
)
SELECT
    product_id,
    PERCENTILE_APPROX(revenue, ARRAY(0.25, 0.5, 0.75)) AS revenue_quartiles,
    AVG(revenue) AS avg_revenue,
    ROUND(AVG(revenue), 2) AS avg_revenue_2
FROM valid
GROUP BY product_id
HAVING AVG(revenue) >= 0
ORDER BY avg_revenue_2 DESC NULLS LAST
