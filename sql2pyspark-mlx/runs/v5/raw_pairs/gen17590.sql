WITH valid AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        SIZE(tags) AS tags_count,
        COALESCE(brand, 'UNKNOWN') AS resolved_brand
    FROM products
    WHERE brand IS DISTINCT FROM 'Acme'
),
joined AS (
    SELECT src.*, d.city
    FROM valid src
    LEFT JOIN suppliers d
        ON src.supplier_id <=> d.id
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category, city ORDER BY stock_qty, id) AS latest_rank
    FROM joined
    QUALIFY latest_rank = 1
),
scored AS (
    SELECT
        *,
        LEAD(tags_count) OVER (PARTITION BY city, brand ORDER BY launch_date, id) AS next_tags_count,
        LAG(cost) OVER (PARTITION BY city, brand ORDER BY launch_date, id) AS prev_cost,
        LEAD(cost) OVER (PARTITION BY city, brand ORDER BY launch_date, id) AS next_cost,
        ROW_NUMBER() OVER (PARTITION BY city, brand ORDER BY launch_date, id) AS seq_no,
        AVG(stock_qty) OVER (PARTITION BY city) AS city_avg_stock_qty
    FROM ranked
),
with_last AS (
    SELECT
        s.*,
        x.quantity,
        x.inventory_last_restock
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.quantity, c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    s.id,
    s.product_name,
    s.category,
    s.brand,
    s.cost,
    s.stock_qty,
    s.launch_date,
    s.tags,
    s.supplier_id,
    CASE WHEN brand RLIKE '^(Globex|Umbrella)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS brand_zone,
    (SELECT MIN(x.cost) FROM products x WHERE x.supplier_id = s.supplier_id) AS min_product_cost
FROM with_last s
ORDER BY launch_date, min_product_cost
