WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        city,
        opened_date,
        sqft,
        CASE WHEN sqft >= 14038 THEN 'High' WHEN sqft >= 7523 THEN 'Medium' ELSE 'Low' END AS sqft_tier,
        TRIM(region) AS clean_region,
        COALESCE(region, 'N/A') AS resolved_region
    FROM stores
    WHERE opened_date >= DATE '2023-07-03'
      AND region IS NOT NULL
)
-- Ranking and gap features
SELECT
    id,
    city,
    sqft,
    resolved_region,
    AVG(sqft) OVER (ORDER BY opened_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS sqft_avg_5_prior,
    LEAD(sqft, 2) OVER (ORDER BY sqft, id) AS next_sqft,
    FIRST_VALUE(sqft, TRUE) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft
FROM cleaned
