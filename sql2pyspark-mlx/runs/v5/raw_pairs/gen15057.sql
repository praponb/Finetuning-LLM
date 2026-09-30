WITH base AS (
    SELECT
        id,
        product_id,
        unit_price,
        GREATEST(quantity, unit_price) greatest_quantity_unit_price,
        quantity + unit_price line_total
    FROM order_items
    WHERE product_id IS NOT NULL
),
with_dims AS (
    SELECT
        e.id,
        e.product_id,
        e.unit_price,
        b.launch_date,
        b.brand,
        b.stock_qty
    FROM base e
    INNER JOIN products b
        ON e.product_id = b.id
),
remaining_keys AS (
    SELECT id
    FROM with_dims
    EXCEPT
    SELECT id
    FROM order_items
    WHERE order_id = 21
),
verified AS (
    SELECT s.*
    FROM with_dims s
    INNER JOIN remaining_keys k ON s.id = k.id
),
valid AS (
    SELECT *, stock_qty - unit_price stock_qty_minus_unit_price
    FROM verified
    WHERE brand IS NOT NULL
),
enriched AS (
    SELECT
        *,
        DENSE_RANK() OVER w_grp stock_qty_minus_unit_price_dense_rank,
        MAX(stock_qty) OVER w_part product_id_max_stock_qty
    FROM valid
    WHERE stock_qty >= 114
    WINDOW w_grp AS (PARTITION BY brand ORDER BY stock_qty_minus_unit_price NULLS FIRST), w_part AS (PARTITION BY product_id)
),
pivoted AS (
    SELECT *
    FROM (
        SELECT
            product_id,
            brand,
            stock_qty_minus_unit_price
        FROM enriched
    ) src
    PIVOT (
        AVG(stock_qty_minus_unit_price) AS value_sum,
        COUNT(1) AS cnt
        FOR brand IN ('Globex', 'Acme')
    )
)
SELECT
    s.product_id,
    s.Globex_value_sum,
    s.Globex_cnt,
    s.Acme_value_sum,
    s.Acme_cnt,
    (SELECT MAX(t2.unit_price) FROM order_items t2 WHERE t2.product_id = s.product_id) AS max_order_item_unit_price
FROM pivoted s
ORDER BY Globex_value_sum DESC;
