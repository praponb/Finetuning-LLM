WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        status,
        DATE_ADD(delivered_date, 74) AS delivered_date_plus_74d,
        COALESCE(carrier, 'UNKNOWN') AS carrier_clean
    FROM shipments
    WHERE delivered_date > '2023-06-01'
),
joined AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.carrier_clean,
        b.discount,
        b.amount,
        b.order_date
    FROM valid a
    LEFT JOIN orders b
        ON a.order_id <=> b.id
    WHERE b.order_date >= '2024-01-11'
),
joined_shipments AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.carrier_clean,
        s.discount,
        s.amount,
        p.discount AS order_discount,
        p.amount AS order_amount
    FROM joined s
    LEFT JOIN orders p
        ON s.order_id = p.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        discount,
        amount,
        order_discount,
        NTILE(3) OVER (PARTITION BY order_id ORDER BY order_amount, id) AS order_amount_bucket,
        FIRST_VALUE(order_discount) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_order_discount,
        NTILE(10) OVER (ORDER BY order_discount, id) AS order_discount_bucket,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_discount DESC, id) AS event_seq,
        LAG(amount, 1, 0) OVER (ORDER BY order_discount, id) AS prev_amount
    FROM joined_shipments
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        discount,
        amount,
        order_discount,
        order_amount_bucket,
        first_known_order_discount,
        order_discount_bucket,
        prev_amount,
        FLOOR(event_seq) AS event_seq_whole
    FROM scored
    WHERE order_discount BETWEEN 0.16 AND 0.34
),
report AS (
    SELECT
        id,
        discount,
        order_discount,
        order_amount_bucket,
        first_known_order_discount,
        order_discount_bucket
    FROM base
)
SELECT order_amount_bucket, order_discount_bucket
FROM report
ORDER BY order_discount_bucket DESC
