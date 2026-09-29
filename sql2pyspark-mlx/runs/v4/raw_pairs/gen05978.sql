SELECT
    CASE WHEN sqft >= 12934 THEN 'LARGE' ELSE 'SMALL' END AS sqft_tier,
    SUM(CASE WHEN city = 'Tokyo' THEN sqft ELSE 0 END) AS tokyo_sqft,
    COUNT(DISTINCT region) AS distinct_regions,
    PERCENTILE_APPROX(sqft, ARRAY(0.25, 0.5, 0.75)) AS sqft_quartiles
FROM stores
GROUP BY CASE WHEN sqft >= 12934 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY tokyo_sqft;
