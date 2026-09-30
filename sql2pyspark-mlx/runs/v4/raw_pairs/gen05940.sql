SELECT
    *,
    RANK() OVER (PARTITION BY city, region ORDER BY sqft DESC NULLS FIRST) sqft_rank,
    ROUND(sqft / SUM(sqft) OVER (PARTITION BY city, region), 4) sqft_share
FROM stores
