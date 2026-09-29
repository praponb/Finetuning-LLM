WITH not_flagged AS (
    SELECT s.*
    FROM suppliers s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.brand NOT IN ('Acme', 'Globex')
        )
),
with_latest AS (
    SELECT
        s.*,
        top1.stock_qty,
        top1.product_price
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.stock_qty, c.price AS product_price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        stock_qty,
        product_price,
        SUM(product_price) OVER (PARTITION BY city ORDER BY product_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_product_price,
        MAX(stock_qty) OVER (PARTITION BY country) AS country_max_stock_qty,
        FIRST_VALUE(city, TRUE) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city
    FROM with_latest
)
SELECT
    id,
    city,
    product_price,
    cumulative_product_price,
    country_max_stock_qty
FROM enriched;
