WITH filtered AS (
    SELECT
        *,
        CONCAT_WS(',', status, channel) AS status_channel_key,
        DATE_FORMAT(order_date, 'MMM yyyy') AS order_date_label
    FROM orders
    WHERE store_id IS NOT NULL
      AND YEAR(order_date) >= 2024
),
scored AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        status_channel_key,
        order_date_label,
        DENSE_RANK() OVER (ORDER BY amount DESC) AS amount_dense_rank,
        PERCENT_RANK() OVER (ORDER BY shipping_fee NULLS LAST) AS shipping_fee_percent_rank
    FROM filtered
),
joined AS (
    SELECT
        e.*,
        d.sqft,
        d.region,
        d.store_name
    FROM scored e
    LEFT JOIN stores d
        ON e.store_id = d.id
),
wide AS (
    SELECT *
    FROM (
        SELECT
            store_id,
            region,
            shipping_fee_percent_rank
        FROM joined
    ) src
    PIVOT (
        SUM(shipping_fee_percent_rank)
        FOR region IN ('South', 'West')
    )
)
SELECT
    s.store_id,
    s.West,
    (SELECT MAX(i2.quantity) FROM inventory i2 WHERE i2.store_id = s.store_id) AS max_inventory_quantity
FROM wide s
ORDER BY West DESC
