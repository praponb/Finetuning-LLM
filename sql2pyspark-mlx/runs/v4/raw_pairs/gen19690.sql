SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    DENSE_RANK() OVER w_recent AS sqft_dense_rank,
    DENSE_RANK() OVER (ORDER BY sqft DESC NULLS LAST) AS sqft_dense_rank_2,
    CUME_DIST() OVER (ORDER BY sqft ASC NULLS FIRST) AS sqft_cume_dist,
    ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share
FROM stores
WINDOW w_recent AS (PARTITION BY region ORDER BY sqft DESC NULLS FIRST);
