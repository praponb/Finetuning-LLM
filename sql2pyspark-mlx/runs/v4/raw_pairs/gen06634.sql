WITH cleaned AS (
    SELECT *, DAYOFWEEK(delivered_date) delivered_date_dayofweek
    FROM shipments
    WHERE delivered_date > DATE '2024-04-12'
      AND order_id IS NOT NULL
      AND shipped_date BETWEEN '2023-07-01' AND '2024-07-09'
),
cleaned_shipments AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        delivered_date_dayofweek,
        SUBSTRING(carrier, 1, 3) carrier_prefix,
        CONCAT_WS('-', status, carrier) status_carrier_key
    FROM cleaned
    WHERE carrier IS NOT NULL
      AND cost >= 41
),
with_dims AS (
    SELECT
        s.*,
        d.discount,
        d.status AS order_status,
        d.amount
    FROM cleaned_shipments s
    LEFT JOIN orders d
        ON s.order_id = d.id
),
enriched AS (
    SELECT a.*, p.status AS order_status_2
    FROM with_dims a
    LEFT JOIN orders p
        ON a.order_id = p.id
),
enriched_shipments AS (
    SELECT a.*, b.shipping_fee
    FROM enriched a
    LEFT JOIN orders b
        ON a.order_id = b.id
)
SELECT
    id,
    order_id,
    carrier,
    delivered_date,
    cost,
    delivered_date_dayofweek,
    status_carrier_key,
    discount,
    amount
FROM enriched_shipments
ORDER BY delivered_date_dayofweek ASC NULLS LAST
