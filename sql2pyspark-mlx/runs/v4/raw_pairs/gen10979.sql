WITH base AS (
    SELECT *, units * revenue AS revenue_per_unit
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-05-01')
      AND product_id IN (2, 4, 10)
),
joined AS (
    SELECT
        src.*,
        p.city,
        p.store_name
    FROM base src
    JOIN stores p
        ON src.store_id = p.id
),
joined_store_sales AS (
    SELECT e.*, d.store_name AS store_store_name
    FROM joined e
    LEFT JOIN stores d
        ON e.store_id = d.id
)
SELECT
    CASE WHEN revenue_per_unit >= 63 THEN 'LARGE' ELSE 'SMALL' END AS revenue_per_unit_tier,
    APPROX_PERCENTILE(revenue, 0.95) AS revenue_p95,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    COUNT_IF(city = 'London') AS london_events,
    ROUND(AVG(revenue), 2) AS avg_revenue,
    APPROX_PERCENTILE(revenue, ARRAY(0.25, 0.5, 0.75)) AS revenue_quartiles,
    SUM(CASE WHEN city = 'Chiang Mai' THEN revenue_per_unit ELSE 0 END) AS chiang_mai_revenue_per_unit
FROM joined_store_sales
GROUP BY CUBE (CASE WHEN revenue_per_unit >= 63 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY london_events DESC;
