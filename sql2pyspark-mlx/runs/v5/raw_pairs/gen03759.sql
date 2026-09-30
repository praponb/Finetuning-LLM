WITH base AS (
    SELECT *, CASE WHEN category IN ('Fashion', 'Books') THEN 'Priority' ELSE 'Other' END AS category_group
    FROM products
    WHERE brand IN ('Globex', 'Initech') AND brand = 'Stark' AND supplier_id = 16
),
category_summary AS (
    SELECT category, COUNT(DISTINCT price) AS unique_categorys, SUM(CASE WHEN brand = 'Acme' THEN cost ELSE 0 END) AS acme_cost
    FROM base
    GROUP BY category
)
SELECT *
FROM category_summary
ORDER BY unique_categorys DESC
