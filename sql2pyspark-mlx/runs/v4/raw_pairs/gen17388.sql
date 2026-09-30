SELECT city, COUNT(*) AS cnt, SIZE(COLLECT_SET(region)) AS num_regions, SUM(sqft) FILTER (WHERE city = 'Bangkok') AS bangkok_total_sqft FROM stores GROUP BY city ORDER BY city ASC NULLS FIRST;
