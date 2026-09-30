WITH filtered AS (
    SELECT
        *,
        discount * shipping_fee AS discount_times_shipping_fee,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM orders
    WHERE channel IS NOT NULL
      AND status = 'CANCELLED'
),
cleaned AS (
    SELECT
        id,
        store_id,
        order_date,
        channel,
        discount,
        shipping_fee,
        discount_times_shipping_fee,
        resolved_channel,
        TRIM(status) AS clean_status,
        TRIM(status) AS clean_status_2
    FROM filtered
    WHERE order_date > DATE '2023-05-27'
      AND store_id IS NOT NULL
      AND store_id IN (6, 12, 22)
),
matched_keys AS (
    SELECT id, channel
    FROM cleaned
    EXCEPT ALL
    SELECT id, channel
    FROM orders
    WHERE discount <= 0.37
),
kept_rows AS (
    SELECT s.*
    FROM cleaned s
    JOIN matched_keys k ON s.id = k.id AND s.channel = k.channel
),
metrics AS (
    SELECT
        id,
        store_id,
        order_date,
        channel,
        discount,
        shipping_fee,
        discount_times_shipping_fee,
        resolved_channel,
        clean_status,
        clean_status_2,
        RANK() OVER (PARTITION BY channel ORDER BY discount_times_shipping_fee DESC NULLS FIRST) AS discount_times_shipping_fee_rank,
        CORR(shipping_fee, discount) OVER (PARTITION BY store_id) AS corr_shipping_fee_discount
    FROM kept_rows
),
kept_keys AS (
    SELECT store_id, channel
    FROM metrics
    INTERSECT ALL
    SELECT store_id, channel
    FROM orders
    WHERE amount >= 1320
),
verified AS (
    SELECT s.*
    FROM metrics s
    JOIN kept_keys k ON s.store_id = k.store_id AND s.channel = k.channel
)
SELECT
    s.id,
    s.store_id,
    s.discount,
    s.discount_times_shipping_fee,
    s.clean_status,
    s.clean_status_2,
    s.discount_times_shipping_fee_rank,
    (SELECT MIN(s2.units) FROM store_sales s2 WHERE s2.store_id = s.store_id) AS min_store_sale_units
FROM verified s;
