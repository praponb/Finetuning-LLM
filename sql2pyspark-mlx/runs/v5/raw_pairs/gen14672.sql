SELECT stores.region, MIN(orders.shipping_fee) AS min_shipping_fee FROM orders LEFT JOIN stores ON stores.id = orders.store_id GROUP BY stores.region ORDER BY stores.region
