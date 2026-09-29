SELECT id, store_id, shipping_fee, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY shipping_fee) AS dense_rnk FROM orders;
