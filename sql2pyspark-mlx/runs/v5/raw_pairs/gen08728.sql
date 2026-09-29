WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        CAST(quantity AS DOUBLE) AS quantity_str
    FROM order_items
    WHERE unit_price >= 130
),
matched_keys AS (
    -- Set intersection on keys
    SELECT product_id, order_id
    FROM filtered
    INTERSECT ALL
    SELECT product_id, order_id
    FROM order_items
    WHERE order_id = 11
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.product_id = k.product_id AND s.order_id = k.order_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY unit_price) AS unit_price_percent_rank,
        STDDEV_SAMP(quantity) OVER (PARTITION BY product_id) AS product_id_stddev_samp_quantity
    FROM kept_rows
    WHERE product_id IN (4, 9, 32)
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.*,
        b.shipping_fee,
        b.status
    FROM metrics a
    INNER JOIN orders b
        ON a.order_id = b.id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        product_id_stddev_samp_quantity,
        shipping_fee,
        status,
        TRIM(status) AS clean_status
    FROM with_dims
    WHERE status IS NOT DISTINCT FROM 'RETURNED'
)
SELECT
    s.id,
    s.order_id,
    s.product_id,
    s.product_id_stddev_samp_quantity,
    s.shipping_fee,
    s.status,
    s.clean_status,
    (SELECT AVG(t2.amount) FROM payments t2 WHERE t2.order_id = s.order_id) AS avg_payment_amount
FROM cleaned s
