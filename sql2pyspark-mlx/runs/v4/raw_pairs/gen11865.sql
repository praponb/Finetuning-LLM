WITH joined AS (
    SELECT a.*, b.country
    FROM products a
    LEFT JOIN suppliers b
        ON a.supplier_id = b.id
)
SELECT
    s.id,
    s.product_name,
    s.brand,
    s.price,
    s.stock_qty,
    s.launch_date,
    s.tags,
    s.supplier_id,
    s.country,
    CASE WHEN brand RLIKE '^(Umbrella|Acme)$' THEN 'OTHER_ZONE' WHEN brand RLIKE '^(Globex|Initech)$' THEN 'PRIORITY' ELSE 'CORE' END AS brand_zone,
    (SELECT AVG(x.stock_qty) FROM products x WHERE x.supplier_id = s.supplier_id) AS avg_product_stock_qty
FROM joined s
ORDER BY avg_product_stock_qty;
