SELECT
    s.region,
    s.opened_date,
    s.sqft,
    ss.store_id,
    ss.revenue,
    ss.units,
    ROUND(ss.revenue / ss.units, 2) AS revenue_per_unit
FROM stores s
LEFT JOIN store_sales ss ON ss.store_id = s.id
WHERE NOT (s.region NOT IN ('West', 'North', 'East') OR s.opened_date <= '2023-11-04')
ORDER BY ss.revenue DESC;
