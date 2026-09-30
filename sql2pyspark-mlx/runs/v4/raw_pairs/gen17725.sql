WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, YEAR(paid_at) AS paid_at_year
    FROM payments
    WHERE method IS NOT NULL
),
joined AS (
    -- Attach orders attributes
    SELECT a.*, o.discount
    FROM cleaned a
    LEFT JOIN orders o
        ON a.order_id = o.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        status,
        paid_at_year,
        discount,
        CEIL(paid_at_year) AS paid_at_year_whole
    FROM joined
    WHERE paid_at >= TIMESTAMP '2023-08-15 01:00:00'
),
enriched AS (
    -- Attach orders attributes
    SELECT
        a.*,
        d.channel,
        d.status AS order_status,
        d.shipping_fee
    FROM base a
    LEFT JOIN orders d
        ON a.order_id = d.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        order_status,
        discount,
        shipping_fee,
        amount
    FROM enriched
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR kpi IN (discount AS DISCOUNT, shipping_fee AS SHIPPING_FEE_VAL, amount)
)
