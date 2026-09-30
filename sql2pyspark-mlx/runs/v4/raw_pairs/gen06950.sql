WITH scored AS (
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
        FIRST_VALUE(cost, TRUE) OVER (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost,
        ROW_NUMBER() OVER (ORDER BY price, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY launch_date DESC, id) AS event_seq_2,
        ROW_NUMBER() OVER (ORDER BY price, id) AS seq_no,
        CORR(stock_qty, price) OVER (PARTITION BY supplier_id) AS corr_stock_qty_price
    FROM products
),
latest_match AS (
    SELECT s.*, x.last_restock
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id, category ORDER BY event_seq_2 DESC, id) AS rank_in_group
        FROM latest_match
    ) x
    WHERE rank_in_group = 1
)
SELECT
    s.id,
    s.product_name,
    s.category,
    s.price,
    s.cost,
    s.launch_date,
    s.tags,
    s.supplier_id,
    s.first_known_cost,
    CASE WHEN product_name RLIKE '^(Desk Lamp|  Water Bottle)$' THEN 'GROUP_B' WHEN product_name RLIKE '^(Laptop Pro 14|wireless mouse)$' THEN 'CORE' ELSE 'PRIORITY' END AS product_name_zone,
    (SELECT MAX(p2.cost) FROM products p2 WHERE p2.supplier_id = s.supplier_id) AS max_product_cost
FROM latest s
ORDER BY max_product_cost NULLS FIRST
