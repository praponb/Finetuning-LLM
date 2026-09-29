WITH base AS (SELECT * FROM store_sales WHERE product_id IN (1, 10)), agg AS (SELECT store_id, COUNT(revenue) AS revenue_count FROM base GROUP BY store_id) SELECT store_id, revenue_count FROM agg;
