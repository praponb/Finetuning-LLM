WITH enriched AS (
    SELECT
        src.id,
        src.order_id,
        src.product_id,
        src.unit_price,
        d.product_name,
        d.brand,
        d.cost
    FROM order_items src
    LEFT JOIN products d
        ON src.product_id = d.id
    WHERE d.supplier_id IS NOT NULL
),
joined AS (
    SELECT
        a.id,
        a.order_id,
        a.product_id,
        a.unit_price,
        a.brand,
        b.product_name AS product_product_name,
        b.brand AS product_brand
    FROM enriched a
    LEFT JOIN products b
        ON a.product_id <=> b.id
),
ranked AS (
    SELECT
        id,
        product_id,
        unit_price,
        brand
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) AS rank_in_group
        FROM joined
    ) t
    WHERE rank_in_group = 1
),
cleaned AS (
    SELECT *, INITCAP(TRIM(brand)) AS brand_title
    FROM ranked
    WHERE product_id IS NOT NULL
)
SELECT
    CASE WHEN unit_price >= 410 THEN 'LARGE' ELSE 'SMALL' END AS unit_price_tier,
    APPROX_PERCENTILE(unit_price, 0.9) AS unit_price_p90,
    COUNT(CASE WHEN brand = 'Acme' THEN 1 END) AS acme_count,
    AVG(unit_price) AS avg_unit_price
FROM cleaned
GROUP BY unit_price_tier
HAVING avg_unit_price >= 0
ORDER BY avg_unit_price DESC NULLS FIRST
