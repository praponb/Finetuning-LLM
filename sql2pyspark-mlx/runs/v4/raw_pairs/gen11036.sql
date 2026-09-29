SELECT
    CASE WHEN sqft >= 17087 THEN 'LARGE' ELSE 'SMALL' END AS sqft_tier,
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    city,
    COUNT(*) FILTER (WHERE city = 'Chiang Mai') AS chiang_mai_rows,
    COUNT(CASE WHEN region = 'South' THEN 1 END) AS south_count,
    SIZE(COLLECT_SET(city)) AS num_cities,
    GROUPING(CASE WHEN sqft >= 17087 THEN 'LARGE' ELSE 'SMALL' END) AS grouping_id
FROM stores
GROUP BY ROLLUP (CASE WHEN sqft >= 17087 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('YEAR', opened_date), city)
