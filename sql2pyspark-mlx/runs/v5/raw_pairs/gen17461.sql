WITH scored AS (
    SELECT
        id,
        status,
        channel,
        discount,
        shipping_fee,
        MIN(shipping_fee) OVER (PARTITION BY customer_id) AS customer_id_min_shipping_fee,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY discount) AS discount_percent_rank,
        CORR(shipping_fee, amount) OVER (PARTITION BY status, store_id) AS corr_shipping_fee_amount
    FROM orders
),
valid AS (
    SELECT
        id,
        status,
        channel,
        discount,
        shipping_fee,
        customer_id_min_shipping_fee,
        discount_percent_rank,
        INITCAP(TRIM(status)) AS status_title,
        CAST(shipping_fee AS INT) AS shipping_fee_int
    FROM scored
    WHERE channel IS NOT NULL
),
base AS (
    SELECT
        id,
        channel,
        discount,
        shipping_fee,
        customer_id_min_shipping_fee,
        discount_percent_rank,
        status_title,
        shipping_fee_int,
        CASE WHEN discount >= 0.4 THEN 'High' WHEN discount >= 0.19 THEN 'Medium' ELSE 'Low' END AS discount_band
    FROM valid
    WHERE customer_id_min_shipping_fee IS NULL
),
remaining_keys AS (
    SELECT id, channel
    FROM base
    EXCEPT
    SELECT id, channel
    FROM orders
    WHERE store_id IN (36, 39)
),
verified AS (
    SELECT s.*
    FROM base s
    JOIN remaining_keys k ON s.id = k.id AND s.channel = k.channel
),
kept AS (
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments ch
            WHERE ch.order_id = s.id
              AND ch.amount < 710
        )
)
SELECT
    channel,
    APPROX_PERCENTILE(shipping_fee_int, 0.5) AS shipping_fee_int_p50,
    CORR(discount, shipping_fee) AS corr_discount_shipping_fee,
    ROUND(SUM(discount) / NULLIF(COUNT(CASE WHEN channel = 'mobile' THEN 1 END), 0), 2) AS discount_per_mobile,
    COUNT(*) FILTER (WHERE channel = 'web') AS web_rows,
    COUNT(*) FILTER (WHERE channel = 'mobile') AS mobile_rows
FROM kept
GROUP BY channel
ORDER BY channel NULLS FIRST
