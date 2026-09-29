SELECT
    *,
    SUM(sqft) OVER (PARTITION BY region) AS region_sum_sqft,
    PERCENT_RANK() OVER (PARTITION BY city ORDER BY sqft DESC NULLS LAST) AS sqft_percent_rank,
    FIRST_VALUE(region, TRUE) OVER (PARTITION BY region, city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_region,
    CUME_DIST() OVER (PARTITION BY region, city ORDER BY sqft DESC NULLS LAST) AS sqft_cume_dist,
    NTILE(5) OVER (ORDER BY sqft, id) AS sqft_bucket
FROM stores;
