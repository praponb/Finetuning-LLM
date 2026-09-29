WITH metrics AS (
    SELECT
        *,
        AVG(sqft) OVER (PARTITION BY region ORDER BY sqft ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) sqft_avg_2_prior,
        AVG(sqft) OVER (PARTITION BY city, region ORDER BY sqft ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) sqft_avg_6_prior
    FROM stores
)
SELECT
    id,
    store_name,
    city,
    region,
    sqft,
    sqft_avg_2_prior,
    sqft_avg_6_prior
FROM metrics
ORDER BY id DESC NULLS LAST, sqft_avg_6_prior ASC
LIMIT 5;
