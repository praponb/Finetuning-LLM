SELECT
    opened_date,
    city,
    LAST_DAY(opened_date) AS opened_date_month_end,
    CONCAT_WS('/', city, region) AS city_region_key
FROM stores
WHERE opened_date BETWEEN '2023-10-01' AND '2025-09-01' OR sqft <= 4979
ORDER BY 4;
