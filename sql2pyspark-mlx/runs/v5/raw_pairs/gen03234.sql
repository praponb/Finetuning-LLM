WITH scored AS (
    SELECT
        *,
        FIRST_VALUE(sqft) IGNORE NULLS OVER (PARTITION BY city, region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft,
        AVG(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS sqft_avg_5
    FROM stores
    WHERE sqft <= 12784
)
SELECT
    id,
    city,
    region,
    opened_date,
    sqft,
    first_known_sqft,
    sqft_avg_5
FROM scored
ORDER BY sqft DESC, sqft_avg_5;
