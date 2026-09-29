WITH cleaned AS (
    SELECT
        *,
        CONCAT(city, ' ', region) AS city_region,
        UPPER(store_name) AS store_name_upper,
        COALESCE(region, 'UNKNOWN') AS region_filled
    FROM stores
    WHERE region IS NOT NULL
)
SELECT
    city,
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    MAX(sqft) FILTER (WHERE region = 'East') AS east_max_sqft,
    PERCENTILE_APPROX(sqft, 0.9) AS sqft_p90,
    PERCENT_RANK() OVER (PARTITION BY city ORDER BY MAX(sqft) FILTER (WHERE region = 'East') DESC) AS east_max_sqft_percent_rank
FROM cleaned
GROUP BY city, DATE_TRUNC('MONTH', opened_date)
