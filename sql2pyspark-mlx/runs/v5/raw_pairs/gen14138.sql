WITH valid AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        GREATEST(quantity, unit_price) AS greatest_quantity_unit_price
    FROM order_items
    WHERE product_id IS NOT NULL
      AND quantity IS NOT NULL
),
with_dims AS (
    SELECT
        e.*,
        p.product_name,
        p.category,
        p.brand
    FROM valid e
    LEFT JOIN products p
        ON e.product_id = p.id
    WHERE ARRAY_CONTAINS(p.tags, 'premium')
),
enriched AS (
    SELECT
        s.id,
        s.product_id,
        s.quantity,
        s.greatest_quantity_unit_price,
        s.product_name,
        s.category,
        s.brand,
        d.brand AS product_brand
    FROM with_dims s
    LEFT JOIN products d
        ON s.product_id = d.id
),
scored AS (
    SELECT
        *,
        SUM(greatest_quantity_unit_price) OVER (PARTITION BY brand ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_greatest_quantity_unit_price,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY greatest_quantity_unit_price, id) AS seq_no,
        AVG(greatest_quantity_unit_price) OVER (PARTITION BY product_brand ORDER BY greatest_quantity_unit_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS greatest_quantity_unit_price_avg_2,
        PERCENT_RANK() OVER (PARTITION BY product_brand ORDER BY greatest_quantity_unit_price) AS greatest_quantity_unit_price_percent_rank
    FROM enriched
)
SELECT
    id,
    quantity,
    greatest_quantity_unit_price,
    product_brand,
    running_greatest_quantity_unit_price,
    seq_no,
    greatest_quantity_unit_price_avg_2,
    greatest_quantity_unit_price_percent_rank,
    CASE WHEN product_brand RLIKE '^(Stark|Acme)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS product_brand_zone
FROM scored
ORDER BY greatest_quantity_unit_price NULLS FIRST, seq_no;
