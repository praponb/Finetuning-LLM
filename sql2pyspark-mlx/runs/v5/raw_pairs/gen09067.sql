SELECT *
FROM (
    SELECT region, QUARTER(opened_date) AS opened_quarter, ROUND(STDDEV(sqft), 2) AS stddev_sqft
    FROM stores
    WHERE store_name LIKE '%Airpor%'
    GROUP BY region, QUARTER(opened_date)
) AS agg
WHERE stddev_sqft > 76;
