SELECT id, store_id, status, shipping_fee, AVG(shipping_fee) OVER (PARTITION BY store_id, status) AS store_id_avg_shipping_fee FROM orders;
