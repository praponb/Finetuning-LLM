SELECT id, product_id, store_id, units, DENSE_RANK() OVER (PARTITION BY product_id, store_id ORDER BY units DESC) AS dense_rnk FROM store_sales WHERE revenue BETWEEN 2700 AND 3000;
