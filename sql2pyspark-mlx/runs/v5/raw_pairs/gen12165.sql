SELECT
    city,
    opened_year,
    unique_regions,
    last_opened_date
FROM (
    SELECT
        city,
        YEAR(opened_date) opened_year,
        COUNT(DISTINCT sqft) unique_regions,
        MAX(opened_date) last_opened_date
    FROM stores
    WHERE sqft <= 14335
    GROUP BY city, YEAR(opened_date)
) t
WHERE unique_regions < 9
