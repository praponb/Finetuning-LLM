WITH latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.sale_date
    FROM products s
    LEFT JOIN LATERAL (
            SELECT c.sale_date
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.sale_date <= s.launch_date
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        e.id,
        e.product_name,
        e.stock_qty,
        e.tags,
        e.supplier_id,
        s.rating,
        s.supplier_name,
        s.country
    FROM latest_match e
    LEFT JOIN suppliers s
        ON e.supplier_id = s.id
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY supplier_id ORDER BY rating DESC NULLS LAST) AS rn
        FROM joined
    ) x
    WHERE rn = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_name,
        stock_qty,
        supplier_id,
        rating,
        supplier_name,
        country,
        rn,
        LAG(stock_qty) OVER w_running AS prev_stock_qty,
        LEAD(stock_qty) OVER w_running AS next_stock_qty,
        LAG(rating) OVER w_running AS prev_rating,
        LEAD(rating) OVER w_running AS next_rating,
        ROW_NUMBER() OVER w_running AS row_seq
    FROM deduped
    WINDOW w_running AS (PARTITION BY supplier_id ORDER BY stock_qty, id)
)
SELECT
    s.id,
    s.product_name,
    s.supplier_id,
    s.rating,
    s.supplier_name,
    s.prev_stock_qty,
    s.next_stock_qty,
    s.prev_rating,
    s.next_rating,
    CASE WHEN product_name RLIKE '^(Running Shoes|Yoga Mat)$' THEN 'GROUP_B' WHEN product_name RLIKE '^(Laptop Pro 14|wireless mouse)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS product_name_zone,
    (SELECT AVG(t2.cost) FROM products t2 WHERE t2.supplier_id = s.supplier_id) AS avg_product_cost
FROM with_windows s;
