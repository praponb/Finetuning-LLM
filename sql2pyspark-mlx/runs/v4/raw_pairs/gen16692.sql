SELECT COUNT(*) AS num_suppliers, ROUND(STDDEV(rating), 2) AS stddev_rating
FROM suppliers
