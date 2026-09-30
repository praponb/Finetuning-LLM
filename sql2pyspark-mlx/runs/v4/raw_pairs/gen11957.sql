WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        order_date,
        status,
        channel,
        shipping_fee,
        CAST(amount AS INT) amount_int,
        COALESCE(channel, 'NONE') channel_clean
    FROM orders
    WHERE store_id IS NOT NULL
      AND channel = 'store'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_date,
        status,
        shipping_fee,
        channel_clean,
        INITCAP(TRIM(status)) status_title,
        COALESCE(channel, 'UNKNOWN') channel_clean_2
    FROM cleaned
    WHERE order_date > DATE '2023-07-06'
      AND store_id IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_date,
        status,
        shipping_fee,
        channel_clean,
        status_title,
        channel_clean_2,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY order_date, id) event_seq,
        FIRST_VALUE(shipping_fee, TRUE) OVER (PARTITION BY status ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_shipping_fee,
        NTILE(3) OVER (PARTITION BY status ORDER BY shipping_fee, id) shipping_fee_bucket
    FROM valid
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_date,
        shipping_fee,
        channel_clean,
        status_title,
        channel_clean_2,
        event_seq,
        first_known_shipping_fee * shipping_fee first_known_shipping_fee_times_shipping_fee
    FROM scored
    WHERE first_known_shipping_fee < 30
),
grouped AS (
    -- Grouped metrics
    SELECT
        CASE WHEN shipping_fee >= 38 THEN 'HIGH' ELSE 'LOW' END AS shipping_fee_tier,
        DATE_TRUNC('YEAR', order_date) AS year_start,
        DATE_TRUNC('WEEK', order_date) AS week_start,
        MIN(event_seq) AS min_event_seq,
        ROUND(AVG(first_known_shipping_fee_times_shipping_fee), 2) AS avg_first_known_shipping_fee_times_shipping_fee,
        MIN(shipping_fee) AS min_shipping_fee
    FROM base
    GROUP BY CASE WHEN shipping_fee >= 38 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('YEAR', order_date), DATE_TRUNC('WEEK', order_date)
)
SELECT
    shipping_fee_tier,
    year_start,
    week_start,
    min_event_seq,
    avg_first_known_shipping_fee_times_shipping_fee,
    min_shipping_fee
FROM grouped;
