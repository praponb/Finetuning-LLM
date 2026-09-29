WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM order_items
    QUALIFY RANK() OVER (PARTITION BY order_id, product_id ORDER BY unit_price DESC NULLS FIRST) = 1
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.quantity,
        e.unit_price,
        b.channel,
        b.order_date
    FROM latest e
    LEFT JOIN orders b
        ON e.order_id = b.id
),
joined AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.unit_price,
        s.order_date,
        b.discount,
        b.amount
    FROM with_dims s
    LEFT JOIN orders b
        ON s.order_id = b.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        unit_price,
        order_date,
        amount,
        MAX(discount) OVER (PARTITION BY order_id ORDER BY order_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_discount,
        COUNT(discount) OVER (PARTITION BY order_id ORDER BY order_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_discount,
        MIN(discount) OVER (PARTITION BY order_id ORDER BY order_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_discount,
        FIRST_VALUE(discount) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_discount
    FROM joined
),
report AS (
    SELECT
        id,
        unit_price,
        last7_max_discount
    FROM scored
)
SELECT unit_price, last7_max_discount
FROM report
ORDER BY last7_max_discount DESC;
