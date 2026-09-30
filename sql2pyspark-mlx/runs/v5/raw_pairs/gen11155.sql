SELECT
    s.region,
    CASE WHEN YEAR(s.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(s.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END AS opened_cohort,
    COUNT(o.discount) AS discount_count,
    MAX(o.shipping_fee) AS max_shipping_fee
FROM orders o
JOIN stores s ON o.store_id = s.id
GROUP BY s.region, CASE WHEN YEAR(s.opened_date) < 2024 THEN 'before_2024' WHEN YEAR(s.opened_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY s.region, opened_cohort
LIMIT 1
