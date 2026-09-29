SELECT
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    region,
    COUNT(*) FILTER (WHERE region = 'East') AS east_rows,
    PERCENTILE_APPROX(sqft, ARRAY(0.25, 0.5, 0.75)) AS sqft_quartiles,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    COUNT(*) AS num_rows,
    GROUPING(DATE_TRUNC('WEEK', opened_date)) AS grouping_id
FROM stores
GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', opened_date), region), (DATE_TRUNC('WEEK', opened_date)))
ORDER BY east_rows;
