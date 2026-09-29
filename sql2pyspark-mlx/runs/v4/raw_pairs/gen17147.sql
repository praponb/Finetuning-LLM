SELECT *
FROM (
    SELECT
        DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month,
        city,
        COUNT(DISTINCT sqft) AS unique_citys,
        SUM(CASE WHEN region = 'East' THEN sqft ELSE 0 END) AS east_sqft
    FROM stores
    GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM'), city
) AS x
WHERE unique_citys < 3
