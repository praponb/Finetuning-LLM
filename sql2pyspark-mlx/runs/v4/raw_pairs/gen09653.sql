WITH enriched AS (
    -- Attach orders attributes
    SELECT
        e.*,
        p.order_date,
        p.status AS order_status,
        p.discount
    FROM payments e
    LEFT JOIN orders p
        ON e.order_id = p.id
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.*,
        p.discount AS order_discount,
        p.order_date AS order_order_date,
        p.status AS order_status_2
    FROM enriched src
    LEFT JOIN orders p
        ON src.order_id = p.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        order_date,
        order_status,
        discount,
        order_discount,
        order_status_2,
        ADD_MONTHS(order_order_date, 6) AS order_order_date_plus_months,
        GREATEST(discount, amount) AS greatest_discount_amount
    FROM with_dims
    WHERE amount BETWEEN 660 AND 960
),
kept_keys AS (
    -- Set intersection on keys
    SELECT status, method
    FROM cleaned
    INTERSECT
    SELECT status, method
    FROM payments
    WHERE amount > 560
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.status = k.status AND s.method = k.method
)
SELECT
    id,
    method,
    amount,
    status,
    order_date,
    order_status,
    discount,
    order_discount
FROM kept_rows
ORDER BY order_discount ASC, discount DESC
