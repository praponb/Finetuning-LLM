WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        GREATEST(unit_price, quantity) AS greatest_unit_price_quantity,
        CAST(unit_price AS INT) AS unit_price_int
    FROM order_items
    WHERE product_id IS NULL
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.*,
        o.shipping_fee,
        o.status,
        o.discount
    FROM base src
    JOIN orders o
        ON src.order_id = o.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        quantity * discount AS quantity_times_discount,
        TRIM(status) AS clean_status
    FROM joined
    WHERE status IS NOT NULL
      AND greatest_unit_price_quantity > 56
)
SELECT
    s.id,
    s.order_id,
    s.product_id,
    s.unit_price,
    s.greatest_unit_price_quantity,
    s.unit_price_int,
    s.status,
    s.discount,
    s.quantity_times_discount,
    (SELECT AVG(t2.units) FROM store_sales t2 WHERE t2.product_id = s.product_id) AS avg_store_sale_units
FROM filtered s
ORDER BY avg_store_sale_units DESC;
