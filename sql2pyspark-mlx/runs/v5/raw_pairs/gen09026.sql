SELECT
    *,
    MIN(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_sqft,
    SUM(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_sqft,
    MAX(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_sqft,
    COUNT(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_sqft,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date, id) AS seq_no
FROM stores
WHERE city <> 'New York'
