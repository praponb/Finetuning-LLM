WITH filtered AS (
    SELECT *, ROUND(revenue * 1.07, 2) AS revenue_scaled
    FROM store_sales
    WHERE sale_date > DATE '2023-06-09'
)
SELECT
    CASE WHEN revenue >= 4400 THEN 'PREMIUM' ELSE 'STANDARD' END AS revenue_tier,
    DATE_TRUNC('WEEK', sale_date) AS week_start,
    store_id,
    ROUND(AVG(revenue), 2) AS avg_revenue,
    MAX(units) AS max_units,
    AVG(revenue) AS avg_revenue_2,
    BIT_AND(units) AS units_bit_and,
    COUNT(DISTINCT product_id) AS distinct_product_ids
FROM filtered
GROUP BY CASE WHEN revenue >= 4400 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', sale_date), store_id
HAVING ROUND(AVG(revenue), 2) < 0
ORDER BY avg_revenue_2 DESC NULLS LAST
