SELECT
    s.region,
    s.city,
    s.sqft,
    ss.revenue,
    ss.sale_date
FROM stores s
LEFT OUTER JOIN store_sales ss ON s.id = ss.store_id
WHERE s.region <> 'East' AND ss.product_id IN (1, 2, 3, 7)
