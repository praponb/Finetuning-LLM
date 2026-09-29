WITH filtered AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        COALESCE(unit_price, 0) AS unit_price_filled
    FROM order_items
    WHERE quantity = 8
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY unit_price_filled DESC, id) AS row_num
    FROM filtered
    QUALIFY row_num = 1
),
scored AS (
    SELECT
        *,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS quantity_avg_3_prior,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS quantity_avg_5,
        AVG(unit_price) OVER w_part AS order_id_avg_unit_price
    FROM latest
    WHERE product_id IS NULL
    WINDOW w_part AS (PARTITION BY order_id)
),
enriched AS (
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        unit_price_filled,
        row_num,
        quantity_avg_3_prior,
        order_id_avg_unit_price,
        NTILE(4) OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) AS unit_price_bucket,
        LEAD(order_id_avg_unit_price, 1, 0) OVER (PARTITION BY order_id ORDER BY quantity, id) AS next_order_id_avg_unit_price
    FROM scored
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_avg_3_prior NULLS LAST, id) AS rank_in_group
        FROM enriched
    ) t
    WHERE rank_in_group = 1
)
SELECT
    s.id,
    s.product_id,
    s.quantity,
    s.unit_price,
    s.unit_price_filled,
    s.row_num,
    s.quantity_avg_3_prior,
    s.order_id_avg_unit_price,
    s.unit_price_bucket,
    (SELECT MIN(t2.units) FROM store_sales t2 WHERE t2.product_id = s.product_id) AS min_store_sale_units
FROM ranked s
ORDER BY quantity DESC NULLS FIRST;
