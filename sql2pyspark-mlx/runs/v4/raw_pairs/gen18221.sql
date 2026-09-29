SELECT
    CASE WHEN sqft >= 15666 THEN 'LARGE' ELSE 'SMALL' END AS sqft_tier,
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    COUNT(DISTINCT city) AS distinct_cities,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    COUNT_IF(region = 'East') AS east_events,
    SIZE(COLLECT_SET(region)) AS num_regions,
    STDDEV_SAMP(sqft) AS sqft_stddev,
    RANK() OVER (PARTITION BY CASE WHEN sqft >= 15666 THEN 'LARGE' ELSE 'SMALL' END ORDER BY COUNT_IF(region = 'East') DESC) AS east_events_rank
FROM stores
GROUP BY CASE WHEN sqft >= 15666 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('WEEK', opened_date)
HAVING COUNT(DISTINCT city) > 1
