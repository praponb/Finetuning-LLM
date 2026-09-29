SELECT MONTH(s.opened_date) AS opened_month, CASE WHEN s.region IN ('North', 'West') THEN 'tier_1' ELSE 'other' END AS region_group, AVG(ss.revenue) AS avg_revenue
FROM store_sales ss
INNER JOIN products p ON p.id = ss.product_id
LEFT JOIN stores s ON ss.store_id = s.id
WHERE ARRAY_CONTAINS(p.tags, 'clearance')
GROUP BY MONTH(s.opened_date), CASE WHEN s.region IN ('North', 'West') THEN 'tier_1' ELSE 'other' END
ORDER BY opened_month DESC, region_group
LIMIT 5;
