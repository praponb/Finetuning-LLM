SELECT
    DATE_TRUNC('MONTH', sale_date) AS activity_month,
    ROUND(AVG(units), 2) AS avg_units,
    PERCENTILE_APPROX(revenue, ARRAY(0.25, 0.5, 0.75)) AS revenue_quartiles,
    ROUND(AVG(units), 2) AS avg_units_2,
    MAX(units) AS max_units,
    COUNT(*) AS event_count,
    STDDEV_SAMP(revenue) AS revenue_stddev
FROM store_sales
GROUP BY DATE_TRUNC('MONTH', sale_date)
HAVING event_count > 1
ORDER BY max_units DESC NULLS LAST;
