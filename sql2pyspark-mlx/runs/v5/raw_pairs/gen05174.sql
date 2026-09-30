SELECT
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    COUNT(CASE WHEN city = 'Singapore' THEN 1 END) AS singapore_count,
    COUNT(*) FILTER (WHERE city = 'London') AS london_rows,
    PERCENTILE_APPROX(sqft, ARRAY(0.25, 0.5, 0.75)) AS sqft_quartiles,
    STDDEV_SAMP(sqft) AS sqft_stddev
FROM stores
GROUP BY DATE_TRUNC('WEEK', opened_date)
ORDER BY sqft_stddev NULLS FIRST;
