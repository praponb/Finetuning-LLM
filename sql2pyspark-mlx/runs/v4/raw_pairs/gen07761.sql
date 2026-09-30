WITH filtered AS (
    SELECT *, unit_price + quantity AS line_total
    FROM order_items
    WHERE product_id IN (10, 21)
),
with_dims AS (
    SELECT
        src.*,
        p.cost,
        p.brand
    FROM filtered src
    LEFT JOIN products p
        ON src.product_id = p.id
),
latest AS (
    SELECT *
    FROM with_dims
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, brand ORDER BY quantity DESC, id) = 1
),
joined AS (
    SELECT
        src.*,
        o.amount,
        o.discount
    FROM latest src
    LEFT JOIN orders o
        ON src.order_id = o.id
),
pivoted AS (
    SELECT *
    FROM (
        SELECT
            product_id,
            brand,
            amount
        FROM joined
    ) src
    PIVOT (
        AVG(amount) AS amount
        FOR brand IN ('Initech', 'Umbrella')
    )
)
SELECT
    product_id,
    Initech,
    Umbrella
FROM pivoted
ORDER BY product_id;
