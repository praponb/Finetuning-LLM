SELECT
    id,
    store_name,
    city,
    opened_date,
    SUM(sqft) OVER (PARTITION BY region ORDER BY sqft ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
    AVG(sqft) OVER (ORDER BY opened_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS sqft_avg_6_prior,
    AVG(sqft) OVER w_ordered AS city_avg_sqft,
    AVG(sqft) OVER (PARTITION BY city, region ORDER BY opened_date ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS sqft_avg_5,
    AVG(sqft) OVER (PARTITION BY region ORDER BY opened_date ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS sqft_avg_5_2
FROM stores
WINDOW w_ordered AS (PARTITION BY city);
