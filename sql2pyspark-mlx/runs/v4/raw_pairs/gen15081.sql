SELECT id, region, opened_date, DENSE_RANK() OVER (PARTITION BY region ORDER BY opened_date) AS dense_rnk FROM stores ORDER BY opened_date DESC
