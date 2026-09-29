SELECT
    *,
    NTILE(5) OVER (PARTITION BY city, region ORDER BY sqft, id) AS sqft_bucket,
    LAST_VALUE(sqft) IGNORE NULLS OVER w_ordered AS last_known_sqft,
    NTILE(3) OVER w_running AS sqft_bucket_2
FROM stores
WINDOW w_ordered AS (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (ORDER BY sqft DESC, id);
