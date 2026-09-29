WITH joined AS (
    SELECT
        src.*,
        b.status AS order_status,
        b.discount,
        b.shipping_fee
    FROM shipments src
    LEFT JOIN orders b
        ON src.order_id = b.id
),
joined_shipments AS (
    SELECT
        src.*,
        d.channel,
        d.shipping_fee AS order_shipping_fee,
        d.amount
    FROM joined src
    LEFT JOIN orders d
        ON src.order_id = d.id
),
latest AS (
    SELECT *
    FROM joined_shipments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_status ORDER BY shipped_date NULLS LAST, id) = 1
),
matched AS (
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM latest s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.shipped_date BETWEEN pr.starts_on AND pr.ends_on
),
joined_shipments_2 AS (
    SELECT
        s.*,
        b.discount AS order_discount,
        b.shipping_fee AS order_shipping_fee_2
    FROM matched s
    JOIN orders b
        ON s.order_id = b.id
)
SELECT *
FROM (
    SELECT
        id,
        promo_id,
        promo_code,
        discount_pct,
        order_discount,
        cost
    FROM joined_shipments_2
) src
UNPIVOT (
    value FOR metric_name IN (discount_pct, order_discount, cost)
);
