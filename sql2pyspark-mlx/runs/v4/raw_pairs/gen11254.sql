SELECT
    DATE_TRUNC('WEEK', sale_date) AS week_start,
    store_id,
    COUNT(*) AS cnt,
    STDDEV_SAMP(units) AS units_stddev,
    STDDEV_SAMP(revenue) AS revenue_stddev
FROM store_sales
GROUP BY DATE_TRUNC('WEEK', sale_date), store_id
