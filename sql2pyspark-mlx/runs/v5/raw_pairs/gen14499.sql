SELECT
    region,
    SUM(sqft) AS total_sqft,
    COUNT(*) AS cnt,
    RANK() OVER (ORDER BY SUM(sqft) DESC) AS sqft_rank
FROM stores
GROUP BY region;
