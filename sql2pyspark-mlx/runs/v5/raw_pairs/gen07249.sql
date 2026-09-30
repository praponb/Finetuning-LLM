WITH cleaned AS (
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
        UPPER(category) AS category_upper
    FROM products
    WHERE launch_date > TO_DATE('2023-05-15')
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY stock_qty, id) AS row_num
        FROM cleaned
    ) ranked
    WHERE row_num = 1
),
latest_match AS (
    SELECT
        s.id,
        s.category,
        s.price,
        s.cost,
        s.stock_qty,
        s.launch_date,
        s.tags,
        s.supplier_id,
        s.category_upper,
        latest.quantity
    FROM latest s
    JOIN LATERAL (
            SELECT c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_dims AS (
    SELECT
        a.id,
        a.category,
        a.cost,
        a.stock_qty,
        a.launch_date,
        a.supplier_id,
        a.quantity,
        p.country
    FROM latest_match a
    INNER JOIN suppliers p
        ON a.supplier_id = p.id
    WHERE p.country <> 'DE'
),
metrics AS (
    SELECT
        *,
        SUM(quantity) OVER (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity,
        FIRST_VALUE(stock_qty, TRUE) OVER w_grp AS first_known_stock_qty,
        AVG(quantity) OVER w_running AS quantity_avg_6,
        LEAD(stock_qty, 1) OVER (ORDER BY cost, id) AS next_stock_qty,
        CUME_DIST() OVER (ORDER BY cost DESC) AS cost_cume_dist
    FROM with_dims
    WINDOW w_grp AS (PARTITION BY supplier_id, country ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY country, category ORDER BY launch_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
enriched AS (
    SELECT
        a.id,
        a.cost,
        a.stock_qty,
        a.launch_date,
        a.supplier_id,
        a.first_known_stock_qty,
        a.quantity_avg_6,
        a.next_stock_qty,
        a.cost_cume_dist,
        p.country AS supplier_country
    FROM metrics a
    LEFT JOIN suppliers p
        ON a.supplier_id = p.id
)
SELECT
    id,
    first_known_stock_qty,
    quantity_avg_6,
    cost_cume_dist
FROM enriched
ORDER BY id DESC NULLS LAST, cost_cume_dist
