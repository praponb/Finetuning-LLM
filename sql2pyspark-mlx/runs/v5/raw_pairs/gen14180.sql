WITH valid AS (
    SELECT *, COALESCE(sqft, 0) sqft_filled
    FROM stores
    WHERE opened_date >= DATE '2023-08-13'
      AND store_name LIKE 'Ri%'
)
SELECT
    id,
    city,
    opened_date,
    sqft,
    AVG(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) sqft_avg_6,
    AVG(sqft_filled) OVER (PARTITION BY region ORDER BY sqft_filled, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) sqft_filled_avg_5,
    SUM(sqft_filled) OVER (PARTITION BY city, region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_sqft_filled
FROM valid;
