SELECT
    id,
    region,
    opened_date,
    city,
    RANK() OVER (PARTITION BY region ORDER BY opened_date DESC) AS rnk,
    RANK() OVER (PARTITION BY city ORDER BY opened_date) AS rnk_2
FROM stores
