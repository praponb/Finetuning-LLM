SELECT
    CASE WHEN p.category IN ('Grocery', 'Fashion') THEN 'tier_1' ELSE 'other' END AS category_group,
    p.brand,
    MAX(ss.revenue) AS max_revenue,
    MIN(ss.sale_date) AS min_sale_date
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
RIGHT JOIN stores s ON s.id = ss.store_id
GROUP BY CASE WHEN p.category IN ('Grocery', 'Fashion') THEN 'tier_1' ELSE 'other' END, p.brand
HAVING MAX(ss.revenue) > 2900
ORDER BY category_group, min_sale_date
