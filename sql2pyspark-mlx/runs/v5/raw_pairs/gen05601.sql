SELECT channel FROM orders WHERE amount >= 1010 UNION ALL SELECT channel FROM orders WHERE shipping_fee IS NOT NULL
