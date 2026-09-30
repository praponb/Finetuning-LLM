SELECT
    city,
    PERCENTILE_APPROX(rating, 0.9) AS rating_p90,
    AVG(rating) FILTER (WHERE country = 'US') AS us_avg_rating,
    PERCENTILE_APPROX(rating, ARRAY(0.25, 0.5, 0.75)) AS rating_quartiles,
    COUNT(*) FILTER (WHERE country = 'TH') AS th_rows
FROM suppliers
GROUP BY city
