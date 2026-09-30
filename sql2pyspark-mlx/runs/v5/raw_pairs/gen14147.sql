WITH enriched AS (
    -- Attach suppliers attributes
    SELECT
        a.id,
        a.product_name,
        a.cost,
        a.launch_date,
        a.supplier_id,
        d.rating,
        d.country,
        d.city
    FROM products a
    INNER JOIN suppliers d
        ON a.supplier_id = d.id
    WHERE d.city = 'Chiang Mai'
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name,
        s.supplier_id,
        s.rating,
        s.city,
        latest.quantity
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock <= s.launch_date
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_name,
        supplier_id,
        rating,
        city,
        MIN(quantity) OVER w_part AS last3_min_quantity,
        COUNT(quantity) OVER w_part AS last3_count_quantity,
        AVG(quantity) OVER w_part AS last3_avg_quantity,
        SUM(quantity) OVER (PARTITION BY supplier_id, city) AS supplier_id_sum_quantity
    FROM latest_match
    WINDOW w_part AS (PARTITION BY city, supplier_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
with_last AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.supplier_id,
        s.city,
        s.last3_min_quantity,
        s.supplier_id_sum_quantity,
        top1.store_sale_units
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.units AS store_sale_units
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    s.id,
    s.supplier_id,
    s.city,
    s.last3_min_quantity,
    s.supplier_id_sum_quantity,
    s.store_sale_units,
    CASE WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'EXTENDED' WHEN city RLIKE '^(New York|Singapore)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS city_zone,
    (SELECT MIN(x.price) FROM products x WHERE x.supplier_id = s.supplier_id) AS min_product_price
FROM with_last s
ORDER BY supplier_id, min_product_price DESC;
